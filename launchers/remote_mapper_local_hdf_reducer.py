import os
import shutil
from common import meassure_time, execute_concurrently, load_hdf_result_file, separate_results
from workers.aws_mapper import launch_worker as launch_mapper
from workers.local_hdf_reducer import launch_worker as launch_reducer
from typing import Dict
import h5py
import lzma
import functools
from common import meassure_time
from converters import Converters
import glob
from datatypes.filesystem import FilesystemBinary, FilesystemHDF
import lzma
from launchers.common.remote_mapper import *

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
SHOULD_MAPPER_PRODUCE_HDF = True
OPERATION = "hdf"

def launch_test(
    how_many_samples: int,
    how_many_workers: int
) -> Dict:
    """
    A function that runs a given test case.
    A test case is described with the arguments listed below.

    Args:
        how_many_samples (int): number of samples that should be generated
        how_many_workers (int): number of workers that should be used for samples generation

    Returns:
        Dict: a dictionary with metrics gathered within the test
    """

    # initial preparation
    metrics = {}
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    os.makedirs(FINAL_RESULTS, exist_ok=True)
    
    # mapping
    mapper_filesystem_results, map_time, worker_times = launch_remote_mapper(how_many_samples, how_many_workers, INPUT_FILES_DIR, TEMPORARY_RESULTS, SHOULD_MAPPER_PRODUCE_HDF)  
    separate_results(mapper_filesystem_results.get_directory(), TEMPORARY_RESULTS)
   
    # reducing
    cumulative_reduce_time = 0
    for subdir in glob.glob(f"{TEMPORARY_RESULTS}/*"):
        _directory_path, just_file_name = os.path.split(subdir)
        output_file_name = f"{just_file_name}.h5"
        reducer_function = functools.partial(
            launch_reducer,
            input_files=FilesystemHDF(subdir),
            output_file_name=output_file_name
        )
        reducer_result, reduce_time = meassure_time(lambda: execute_concurrently(reducer_function, 1))
        cumulative_reduce_time += reduce_time
        if subdir.endswith("/z_profile"):
            metrics["hdf_results"] = reducer_result[0].to_memory().read("z_profile.h5")
        reducer_result[0].to_filesystem(FINAL_RESULTS)
    
    # update metrics
    metrics["reduce_time"] = cumulative_reduce_time
    metrics["map_time"] = map_time
    metrics["worker_times"] = worker_times
    
    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)
    return metrics
  
