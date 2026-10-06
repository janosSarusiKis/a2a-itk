"""The slimrpc binding: the gRPC binding's service, carried over SLIM.

slimrpc is SLIM's RPC layer. An A2A agent served over it registers the same
``lf.a2a.v1.A2AService`` as the gRPC binding — same methods, same request and
response messages — so everything above the channel is the gRPC dispatcher's
and is inherited from it: request construction, ProtoJSON encoding with
required-field restoration, and the agent card over HTTP. What differs is the
channel, and with it:

- **Addressing.** The card's interface ``url`` is a SLIM name,
  ``namespace/group/name``, not ``host:port``. Both the runner and the SUT
  connect to a SLIM node, which routes between their names; the dispatcher
  starts that node for the run (:meth:`SlimRpcDispatcher.sut_environment`).
- **Errors.** A failed call raises ``slim_bindings.RpcError.Rpc`` carrying a
  ``google.rpc.Code`` and, optionally, a serialized ``google.rpc.Status``.
  The status name is read from the code exactly as gRPC's is, so the
  canonical transcoding to an HTTP status applies unchanged, and the abstract
  error name comes from the Status's ``ErrorInfo`` when the SUT sends one.
- **Metadata casing.** Kept as the caller wrote it. gRPC lowercases because
  HTTP/2 requires it; SLIM imposes nothing, and an A2A server over slimrpc
  looks headers up as spelled (``A2A-Extensions``).
- **No mid-stream cancel.** The response stream reader has no cancel, so a
  step that stops reading early stops consuming, but the SUT is not told.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import tempfile
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Mapping

import httpx
from google.rpc import code_pb2

try:
    import slim_bindings
except ModuleNotFoundError as exc:  # pragma: no cover - depends on the install
    raise ModuleNotFoundError(
        'the slimrpc binding needs the optional `slimrpc` extra: '
        '`uv run --extra slimrpc run_acts.py --transport slimrpc ...`',
        name=exc.name,
    ) from exc

from test_suite.acts.dispatcher.base import (
    DispatchError,
    StreamEvent,
    StreamNotOpened,
    UnsupportedByBinding,
    WireError,
    WireResponse,
)
from test_suite.acts.dispatcher.grpc import (
    GrpcDispatcher,
    _message_type,
    _to_dict,
    reason_from_status,
)
from test_suite.acts.schema import Operation, RawBlock, TransportBinding
from test_suite.acts.wire_map import (
    GRPC_SERVICE,
    binding_for_operation,
    error_for_reason,
    http_status_for_grpc,
    resolve_operation,
)
from test_suite.launcher.ports import free_port


#: Shared secret the runner and SUT both create their SLIM apps with. The
#: default is slima2a's own; exported to the SUT so the two always agree.
SHARED_SECRET = os.environ.get(
    'SLIM_SHARED_SECRET', 'secretsecretsecretsecretsecretsecret'
)

#: Where the dispatcher finds the SLIM node when nothing says otherwise.
DEFAULT_ENDPOINT = 'http://127.0.0.1:46357'

#: The SLIM name the runner registers under; the last component is unique
#: per dispatcher so concurrent runs on one node never share a name.
CLIENT_NAMESPACE = 'agntcy'
CLIENT_GROUP = 'itk'

#: How long the node gets to start listening.
NODE_START_TIMEOUT_S = 15.0

_ITK_ROOT = Path(__file__).resolve().parents[3]


def _status_name(code: Any) -> str:
    """The gRPC status name a slimrpc error code stands for.

    Arrives as ``slim_bindings.RpcCode``, an enum named after the gRPC codes;
    a bare ``google.rpc.Code`` number is accepted too.
    """
    name = getattr(code, 'name', None)
    if isinstance(name, str):
        return name
    try:
        return code_pb2.Code.Name(int(code))
    except (TypeError, ValueError):
        return 'UNKNOWN'


def _response_from_rpc_error(exc: Any) -> WireResponse:
    """Turn a failed slimrpc call into a :class:`WireResponse`."""
    status_name = _status_name(getattr(exc, 'code', None))
    blob = getattr(exc, 'details', None)
    reason, details = (
        reason_from_status(blob) if isinstance(blob, bytes) and blob else (None, ())
    )
    return WireResponse(
        status=http_status_for_grpc(status_name),
        payload=None,
        error=WireError(
            message=str(getattr(exc, 'message', '') or ''),
            error_type=error_for_reason(reason) if reason else None,
            status=status_name,
            reason=reason,
            details=details,
            raw=exc,
        ),
        headers={},
    )


def _describe(exc: Any) -> str:
    return f"{_status_name(getattr(exc, 'code', None))}: {getattr(exc, 'message', exc)}"


@contextlib.asynccontextmanager
async def _slim_node() -> AsyncIterator[dict[str, str]]:
    """Run a SLIM node on a free loopback port for the length of a run."""
    port = free_port()
    endpoint = f'127.0.0.1:{port}'
    with tempfile.TemporaryFile() as log:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'test_suite.acts.dispatcher.slim_node', endpoint,
            cwd=_ITK_ROOT,
            stdout=log,
            stderr=log,
        )
        try:
            await _wait_listening(port, proc, log)
            yield {
                'SLIM_ENDPOINT': f'http://{endpoint}',
                'SLIM_SHARED_SECRET': SHARED_SECRET,
            }
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()


async def _wait_listening(port: int, proc: Any, log: Any) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + NODE_START_TIMEOUT_S
    while loop.time() < deadline:
        if proc.returncode is not None:
            log.seek(0)
            tail = log.read().decode(errors='replace')[-2000:]
            raise DispatchError(
                f'the SLIM node exited with {proc.returncode} before listening:\n{tail}'
            )
        try:
            _, writer = await asyncio.open_connection('127.0.0.1', port)
        except OSError:
            await asyncio.sleep(0.1)
            continue
        writer.close()
        await writer.wait_closed()
        return
    raise DispatchError(f'the SLIM node did not listen on :{port} within {NODE_START_TIMEOUT_S}s')


#: One connection per SLIM node for the whole process. The service refuses a
#: second connect to an endpoint it is already connected to, and a run builds
#: several dispatchers — one per pass — against the same node.
_CONNECTIONS: dict[str, int] = {}


async def _connection(service: Any, endpoint: str) -> int:
    if endpoint not in _CONNECTIONS:
        _CONNECTIONS[endpoint] = await service.connect_async(
            slim_bindings.new_insecure_client_config(endpoint)
        )
    return _CONNECTIONS[endpoint]


class SlimRpcDispatcher(GrpcDispatcher):
    """Dispatches ACTS operations over slimrpc."""

    binding = TransportBinding.SLIMRPC

    def __init__(
        self,
        target: str,
        *,
        agent_card_url: str,
        slim_endpoint: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        timeout: float = 30.0,
        default_headers: Mapping[str, str] | None = None,
    ) -> None:
        """
        Args:
          target: The SUT's SLIM name, ``namespace/group/name``.
          agent_card_url: Base URL for the well-known agent card, which is
            served over HTTP: slimrpc has no card RPC either.
          slim_endpoint: The SLIM node to connect through. Defaults to
            ``$SLIM_ENDPOINT``, which :meth:`sut_environment` sets for a run.
        """
        parts = target.split('/')
        if len(parts) != 3 or not all(parts):
            raise DispatchError(
                f'a slimrpc interface url is a SLIM name `namespace/group/name`, '
                f'got {target!r}'
            )
        # Not GrpcDispatcher.__init__: there is no gRPC channel to open.
        self.target = target
        self.timeout = timeout
        self._remote = parts
        self._endpoint = slim_endpoint
        self._default_headers = dict(default_headers or {})
        self._agent_card_url = agent_card_url.rstrip('/')
        self._owns_http = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=timeout)
        self._channel: Any = None
        self._connect_lock = asyncio.Lock()

    @classmethod
    def from_interface(
        cls,
        url: str,
        *,
        agent_card_url: str,
        default_headers: Mapping[str, str] | None = None,
    ) -> SlimRpcDispatcher:
        return cls(url, agent_card_url=agent_card_url, default_headers=default_headers)

    @classmethod
    def sut_environment(cls) -> contextlib.AbstractAsyncContextManager[dict[str, str]]:
        return _slim_node()

    async def aclose(self) -> None:
        if self._channel is not None:
            with contextlib.suppress(Exception):
                await self._channel.close_async()
            self._channel = None
        if self._owns_http:
            await self._http.aclose()

    # -- the SLIM connection -----------------------------------------------

    async def _connected(self) -> Any:
        """The channel to the SUT, opened on first use.

        Construction is synchronous, as every dispatcher's is, while joining
        a SLIM node is not, so the connection waits for the first call.
        """
        async with self._connect_lock:
            if self._channel is not None:
                return self._channel

            slim_bindings.uniffi_set_event_loop(asyncio.get_running_loop())
            if not slim_bindings.is_initialized():
                slim_bindings.initialize_with_configs(
                    runtime_config=slim_bindings.new_runtime_config(),
                    tracing_config=slim_bindings.new_tracing_config(),
                    service_config=[slim_bindings.new_service_config()],
                )
            service = slim_bindings.get_global_service()
            endpoint = self._endpoint or os.environ.get('SLIM_ENDPOINT', DEFAULT_ENDPOINT)
            try:
                conn_id = await _connection(service, endpoint)
                local = slim_bindings.Name(
                    CLIENT_NAMESPACE, CLIENT_GROUP, f'acts-runner-{uuid.uuid4().hex[:12]}'
                )
                # The synchronous constructor, as slima2a's own helper uses:
                # `create_app_with_secret_async` panics in slim-bindings 2.x
                # for want of a Tokio reactor on the calling thread.
                app = service.create_app_with_secret(local, SHARED_SECRET)
                await app.subscribe_async(local, conn_id)
            except Exception as exc:  # noqa: BLE001 - any failure here is one fact
                raise DispatchError(
                    f'cannot join the SLIM node at {endpoint}: {exc}'
                ) from exc

            self._channel = slim_bindings.Channel.new_with_connection(
                app, slim_bindings.Name(*self._remote), conn_id
            )
            return self._channel

    def _metadata(self, headers: Mapping[str, str] | None) -> dict[str, str]:  # type: ignore[override]
        return {**self._default_headers, **(headers or {})}

    # -- the Dispatcher contract -------------------------------------------

    async def dispatch(
        self,
        operation: Operation,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> WireResponse:
        operation = resolve_operation(operation, params)
        binding = binding_for_operation(operation)
        if binding.http_only:
            return await self._get_agent_card(headers)
        if binding.streaming:
            raise DispatchError(
                f'{operation.value} is a streaming operation; use stream()'
            )

        request = self._build_request(operation, params)
        channel = await self._connected()
        try:
            raw = await channel.call_unary_async(
                GRPC_SERVICE,
                binding.grpc_method,
                request.SerializeToString(),
                timedelta(seconds=self.timeout),
                self._metadata(headers),
            )
        except slim_bindings.RpcError.Rpc as exc:  # type: ignore[attr-defined]
            return _response_from_rpc_error(exc)
        except slim_bindings.RpcError as exc:
            raise DispatchError(f'slimrpc call failed: {exc}') from exc

        response = _message_type(binding.grpc_response).FromString(raw)
        return WireResponse(status=200, payload=_to_dict(response), headers={})

    async def stream(
        self,
        operation: Operation,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        binding = binding_for_operation(operation)
        if not binding.streaming:
            raise DispatchError(
                f'{operation.value} is not a streaming operation; use dispatch()'
            )

        request = self._build_request(operation, params)
        message_type = _message_type(binding.grpc_response)
        channel = await self._connected()
        try:
            reader = await channel.call_unary_stream_async(
                GRPC_SERVICE,
                binding.grpc_method,
                request.SerializeToString(),
                timedelta(seconds=self.timeout),
                self._metadata(headers),
            )
        except slim_bindings.RpcError.Rpc as exc:  # type: ignore[attr-defined]
            raise StreamNotOpened(
                f'the SUT refused the stream: {_describe(exc)}',
                _response_from_rpc_error(exc),
            ) from exc

        index = 0
        while True:
            item = await reader.next_async()
            if item.is_end():
                return
            if item.is_error():
                exc = item[0]
                if index == 0:
                    # As on gRPC: nothing streamed means the SUT refused the
                    # call, and the error says which refusal it was.
                    raise StreamNotOpened(
                        f'the SUT refused the stream: {_describe(exc)}',
                        _response_from_rpc_error(exc),
                    )
                raise DispatchError(
                    f'stream failed after {index} event(s): {_describe(exc)}'
                )
            if item.is_data():
                yield StreamEvent(index=index, data=_to_dict(message_type.FromString(item[0])))
                index += 1

    async def dispatch_raw(
        self,
        raw: RawBlock,
        headers: Mapping[str, str] | None = None,
    ) -> WireResponse:
        raise UnsupportedByBinding(
            'slimrpc carries protobuf messages, so it has no raw-request form; '
            'a test built from `raw` steps names its transport (ACTS §4.4)'
        )

    async def stream_raw(
        self,
        raw: RawBlock,
        headers: Mapping[str, str] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        raise UnsupportedByBinding(
            'slimrpc has no raw-request form, streaming or otherwise (ACTS §4.4)'
        )
        yield  # pragma: no cover - unreachable, but makes this a generator
