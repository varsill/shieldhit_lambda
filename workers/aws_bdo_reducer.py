import os

from common import meassure_time
from datatypes.filesystem import FilesystemHDF
from datatypes.in_memory import InMemoryBinary, InMemoryHDF
from workers.common.remote_reducer_invocation_api import \
    send_request_to_remote_reducer


def launch_worker(
    files: InMemoryBinary, operation: str, get_from: str, worker_id_prefix: str = ""
):

    reducer_results = send_request_to_remote_reducer(
        files, operation, os.getenv("AWS_LAMBDA_URL"), worker_id_prefix, get_from
    )

    return (
        reducer_results["files"],
        reducer_results["simulation_time"],
        reducer_results["request_time"],
    )
