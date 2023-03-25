from common import meassure_time
from datatypes.in_memory import InMemoryBinary
import os

def launch_worker(files: InMemoryBinary):
    from workers.common.remote_reducer_invocation_api import (
        send_request_to_remote_hdf_reducer
    )
    reducer_results, reduce_time = meassure_time(lambda: send_request_to_remote_hdf_reducer(
        files,
        os.getenv("WHISK_ACTION_URL")
    ))
    return reducer_results["files"], reduce_time