from common import meassure_time
from datatypes.filesystem import FilesystemHDF
from datatypes.in_memory import InMemoryBinary
import os
import lzma


def launch_worker(files_map, output_dir, operation):
    from workers.common.remote_reducer_invocation_api import (
        send_request_to_remote_reducer
    )
    reducer_results, reduce_time = meassure_time(lambda: send_request_to_remote_reducer(
        files_map,
        operation,
        os.getenv("AWS_LAMBDA_URL")
    ))
    InMemoryBinary(reducer_results["files"], lzma.decompress).to_filesystem(output_dir)
    reducer_results = FilesystemHDF(output_dir)
    hdf_sample = reducer_results.to_memory().read("z_profile_.h5")
    return reducer_results, reduce_time, hdf_sample