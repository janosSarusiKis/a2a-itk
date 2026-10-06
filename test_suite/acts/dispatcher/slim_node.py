"""A SLIM node for one conformance run: ``python -m ... ENDPOINT``.

The slimrpc binding is brokered: the runner and the SUT never connect to each
other, both connect to a SLIM node that routes between their names. This is
that node, hosted by ``slim_bindings`` itself so a run needs no separate
binary or container. :meth:`SlimRpcDispatcher.sut_environment` starts it as a
subprocess and stops it when the run ends.

Insecure on purpose: it listens on loopback for the length of one run, and
the SUT and runner still authenticate to each other with the shared secret.
"""

from __future__ import annotations

import asyncio
import sys

import slim_bindings


async def serve(endpoint: str) -> None:
    slim_bindings.uniffi_set_event_loop(asyncio.get_running_loop())
    slim_bindings.initialize_with_configs(
        runtime_config=slim_bindings.new_runtime_config(),
        tracing_config=slim_bindings.new_tracing_config(),
        service_config=[slim_bindings.new_service_config()],
    )
    service = slim_bindings.get_global_service()
    await service.run_server_async(slim_bindings.new_insecure_server_config(endpoint))
    print(f'SLIM node listening on {endpoint}', flush=True)
    await asyncio.Event().wait()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit(f'usage: python -m {__spec__.name} HOST:PORT')
    asyncio.run(serve(sys.argv[1]))
