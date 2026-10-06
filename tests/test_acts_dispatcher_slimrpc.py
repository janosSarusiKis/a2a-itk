"""The slimrpc adapter: the gRPC binding's service carried over SLIM.

Needs the optional `slimrpc` extra (`uv run --extra slimrpc pytest`); skipped
without it. The round-trip tests run a real SLIM node and a real slimrpc
server in-process, for the same reason the gRPC tests pay for a real server:
a fake channel would let a wrong service name or message type pass.
"""

from __future__ import annotations

import asyncio
import os
import socket

import pytest

slim_bindings = pytest.importorskip('slim_bindings')

from google.protobuf.any_pb2 import Any as ProtoAny  # noqa: E402
from google.rpc import error_details_pb2, status_pb2  # noqa: E402

from pyproto import a2a_pb2  # noqa: E402

from test_suite.acts.dispatcher import (  # noqa: E402
    DispatchError,
    SlimRpcDispatcher,
    UnsupportedByBinding,
)
from test_suite.acts.dispatcher.slimrpc import (  # noqa: E402
    SHARED_SECRET,
    _response_from_rpc_error,
    _status_name,
)
from test_suite.acts.schema import (  # noqa: E402
    HttpMethod,
    Operation,
    RawBlock,
    TransportBinding,
)
from test_suite.acts.wire_map import (  # noqa: E402
    GRPC_SERVICE,
    error_for_reason,
    http_status_for_grpc,
)

SUT_NAME = ('agntcy', 'itk', 'fake-sut')
CARD_URL = 'http://127.0.0.1:9'


def status_blob(code: int, reason: str) -> bytes:
    detail = ProtoAny()
    detail.Pack(error_details_pb2.ErrorInfo(reason=reason, domain='a2a-protocol.org'))
    status = status_pb2.Status(code=code, message='boom')
    status.details.append(detail)
    return status.SerializeToString()


class TestTarget:
    def test_the_target_is_a_slim_name(self):
        dispatcher = SlimRpcDispatcher('agntcy/itk/sut', agent_card_url=CARD_URL)
        assert dispatcher.binding is TransportBinding.SLIMRPC
        assert dispatcher.target == 'agntcy/itk/sut'
        asyncio.run(dispatcher.aclose())

    @pytest.mark.parametrize('target', ['127.0.0.1:50051', 'agntcy/itk', 'a//b'])
    def test_anything_else_is_refused(self, target):
        with pytest.raises(DispatchError, match='namespace/group/name'):
            SlimRpcDispatcher(target, agent_card_url=CARD_URL)

    def test_from_interface_takes_the_card_url_as_the_name(self):
        dispatcher = SlimRpcDispatcher.from_interface(
            'agntcy/itk/sut', agent_card_url=CARD_URL
        )
        assert dispatcher.target == 'agntcy/itk/sut'
        asyncio.run(dispatcher.aclose())


class TestErrors:
    @pytest.mark.parametrize(
        ('code', 'name'),
        [
            (slim_bindings.RpcCode.NOT_FOUND, 'NOT_FOUND'),
            (5, 'NOT_FOUND'),
            (9, 'FAILED_PRECONDITION'),
            (None, 'UNKNOWN'),
        ],
    )
    def test_status_name(self, code, name):
        assert _status_name(code) == name

    def test_errorinfo_in_details_names_the_error(self):
        exc = slim_bindings.RpcError.Rpc(
            code=slim_bindings.RpcCode.NOT_FOUND,
            message='TaskNotFoundError: Task not found',
            details=status_blob(5, 'TASK_NOT_FOUND'),
        )
        response = _response_from_rpc_error(exc)
        assert response.status == http_status_for_grpc('NOT_FOUND')
        assert response.error.status == 'NOT_FOUND'
        assert response.error.reason == 'TASK_NOT_FOUND'
        assert response.error.error_type is error_for_reason('TASK_NOT_FOUND')

    def test_without_details_the_error_is_reported_unnamed(self):
        exc = slim_bindings.RpcError.Rpc(
            code=slim_bindings.RpcCode.NOT_FOUND, message='gone', details=None
        )
        response = _response_from_rpc_error(exc)
        assert response.error.status == 'NOT_FOUND'
        assert response.error.reason is None
        assert response.error.error_type is None


class TestShape:
    def test_metadata_keeps_the_callers_casing(self):
        dispatcher = SlimRpcDispatcher(
            'agntcy/itk/sut',
            agent_card_url=CARD_URL,
            default_headers={'Authorization': 'Bearer t'},
        )
        assert dispatcher._metadata({'A2A-Extensions': 'x'}) == {
            'Authorization': 'Bearer t',
            'A2A-Extensions': 'x',
        }
        asyncio.run(dispatcher.aclose())

    def test_there_is_no_raw_form(self):
        dispatcher = SlimRpcDispatcher('agntcy/itk/sut', agent_card_url=CARD_URL)
        raw = RawBlock(method=HttpMethod.POST, path='/', body='{}')
        with pytest.raises(UnsupportedByBinding, match='slimrpc'):
            asyncio.run(dispatcher.dispatch_raw(raw))
        asyncio.run(dispatcher.aclose())


# -- against a real node -----------------------------------------------------


def _listening(endpoint: str) -> bool:
    host, port = endpoint.removeprefix('http://').rsplit(':', 1)
    with socket.socket() as probe:
        return probe.connect_ex((host, int(port))) == 0


class FakeSut:
    """Answers GetTask: a task for id `t-1`, TASK_NOT_FOUND for anything else."""

    async def get_task(self, request: bytes, context: object) -> bytes:
        parsed = a2a_pb2.GetTaskRequest.FromString(request)
        if parsed.id != 't-1':
            raise slim_bindings.RpcError.Rpc(
                code=slim_bindings.RpcCode.NOT_FOUND,
                message='Task not found',
                details=status_blob(5, 'TASK_NOT_FOUND'),
            )
        task = a2a_pb2.Task(
            id='t-1',
            context_id='c-1',
            status=a2a_pb2.TaskStatus(state=a2a_pb2.TaskState.TASK_STATE_COMPLETED),
        )
        return task.SerializeToString()


class _UnaryHandler(slim_bindings.UnaryUnaryHandler):
    def __init__(self, fn):
        self._fn = fn

    async def handle(self, request: bytes, context: object) -> bytes:
        return await self._fn(request, context)


async def _serve_fake_sut(endpoint: str) -> object:
    slim_bindings.uniffi_set_event_loop(asyncio.get_running_loop())
    if not slim_bindings.is_initialized():
        slim_bindings.initialize_with_configs(
            runtime_config=slim_bindings.new_runtime_config(),
            tracing_config=slim_bindings.new_tracing_config(),
            service_config=[slim_bindings.new_service_config()],
        )
    service = slim_bindings.get_global_service()
    from test_suite.acts.dispatcher.slimrpc import _connection

    conn_id = await _connection(service, endpoint)
    name = slim_bindings.Name(*SUT_NAME)
    app = service.create_app_with_secret(name, SHARED_SECRET)
    await app.subscribe_async(name, conn_id)
    server = slim_bindings.Server.new_with_connection(app, name, conn_id)
    server.register_unary_unary(
        service_name=GRPC_SERVICE,
        method_name='GetTask',
        handler=_UnaryHandler(FakeSut().get_task),
    )
    return asyncio.create_task(server.serve_async())


class TestAgainstANode:
    def test_the_environment_runs_a_node_for_the_run(self):
        async def go():
            async with SlimRpcDispatcher.sut_environment() as env:
                assert env['SLIM_SHARED_SECRET'] == SHARED_SECRET
                assert _listening(env['SLIM_ENDPOINT'])
                return env['SLIM_ENDPOINT']

        endpoint = asyncio.run(go())
        assert not _listening(endpoint)

    def test_a_round_trip(self):
        found, missing = asyncio.run(_round_trip())
        assert found.status == 200
        assert found.payload['status']['state'] == 'TASK_STATE_COMPLETED'
        assert missing.status == http_status_for_grpc('NOT_FOUND')
        assert missing.error.status == 'NOT_FOUND'

    @pytest.mark.xfail(
        strict=True,
        reason='slimrpc drops RpcError.details on the wire: send_error_for_rpc '
               'in agntcy/slim crates/rpc/src/rpc_session.rs sends only the '
               'code and message. Remove this marker once SLIM forwards them.',
    )
    def test_errorinfo_survives_the_trip(self):
        _, missing = asyncio.run(_round_trip())
        assert missing.error.reason == 'TASK_NOT_FOUND'


async def _round_trip():
    """GetTask for a task that exists and one that does not, over a real node."""
    async with SlimRpcDispatcher.sut_environment() as env:
        serving = await _serve_fake_sut(env['SLIM_ENDPOINT'])
        dispatcher = SlimRpcDispatcher(
            '/'.join(SUT_NAME),
            agent_card_url=CARD_URL,
            slim_endpoint=env['SLIM_ENDPOINT'],
            timeout=10,
        )
        try:
            found = await dispatcher.dispatch(Operation.GET_TASK, {'id': 't-1'})
            missing = await dispatcher.dispatch(Operation.GET_TASK, {'id': 'nope'})
        finally:
            await dispatcher.aclose()
            serving.cancel()
        return found, missing


@pytest.fixture(autouse=True)
def _no_inherited_endpoint(monkeypatch):
    """Tests name their node explicitly; a stray $SLIM_ENDPOINT must not leak in."""
    monkeypatch.delenv('SLIM_ENDPOINT', raising=False)
    yield
    os.environ.pop('SLIM_ENDPOINT', None)
