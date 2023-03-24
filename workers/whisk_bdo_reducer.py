from common import deserialize, meassure_time
from datatypes.filesystem import FilesystemHDF
import os

def launch_worker(files_map, output_dir, operation):
    from workers.common.remote_reducer_invocation_api import (
        send_request_to_remote_reducer
    )

    reducer_results, reduce_time = meassure_time(lambda: send_request_to_remote_reducer(
        files_map,
        operation,
        os.getenv("WHISK_ACTION_URL")
    ))
    deserialize(reducer_results["files"], output_dir)
    reducer_results = FilesystemHDF(reducer_results)
    hdf_sample = reducer_results.to_memory().read("z_profile_.h5")
    return reducer_results, reduce_time, hdf_sample