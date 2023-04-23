import os
from datatypes.in_memory import InMemoryBinary


def extract_and_reduce(
    files: InMemoryBinary, get_from: str, worker_id_prefix: str = ""
):
    from workers.common.remote_invocation_api import send_request_to_remote_extract_and_reduce
    reducer_results = send_request_to_remote_extract_and_reduce(
        files, os.getenv("AWS_LAMBDA_URL"), worker_id_prefix, get_from
    )

    return (
        reducer_results["files"],
        reducer_results["execution_time"],
        reducer_results["request_time"],
    )
