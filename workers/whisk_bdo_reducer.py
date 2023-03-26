from common import meassure_time
from datatypes.filesystem import FilesystemHDF
from datatypes.in_memory import InMemoryBinary, InMemoryHDF
import os
import lzma

def launch_worker(files, operation, worker_id = ""):
    from workers.common.remote_reducer_invocation_api import (
        send_request_to_remote_reducer
    )

    reducer_results, reduce_time = meassure_time(lambda: send_request_to_remote_reducer(
        files.read_all(),
        operation,
        os.getenv("WHISK_ACTION_URL"), 
        worker_id
    ))
    return  reducer_results["files"], reduce_time