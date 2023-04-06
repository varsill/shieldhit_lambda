import os

from datatypes.in_memory import InMemoryBinary


def launch_worker(
    files: InMemoryBinary, operation: str, get_from: str, worker_id_prefix: str = ""
):
    from workers.common.remote_reducer_invocation_api import send_request_to_remote_reducer
    reducer_results = send_request_to_remote_reducer(
        files, operation, os.getenv("AWS_LAMBDA_URL"), worker_id_prefix, get_from
    )

    return (
        reducer_results["files"],
        reducer_results["simulation_time"],
        reducer_results["request_time"],
    )
