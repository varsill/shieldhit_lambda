from common import meassure_time
from datatypes.filesystem import FilesystemHDF
from datatypes.in_memory import InMemoryBinary, InMemoryHDF
import os
import lzma


def launch_worker(files: InMemoryBinary, operation: str):
    from workers.common.remote_reducer_invocation_api import (
        send_request_to_remote_reducer
    )
    reducer_results, reduce_time = meassure_time(lambda: send_request_to_remote_reducer(
        files,
        operation,
        os.getenv("AWS_LAMBDA_URL")
    ))
    reducer_hdf_results = reducer_results["files"].to_hdf()
    return reducer_hdf_results, reduce_time