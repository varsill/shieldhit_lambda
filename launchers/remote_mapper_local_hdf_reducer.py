import lzma
import os
import shutil
from typing import Dict
import time

from datatypes.filesystem import FilesystemBinary, FilesystemHDF
from launchers.common import prepare_multiple_remote_mappers_function, initialize_metrics
from workers.common.remote import RemoteEnvironment
from workers.common.remote_mapper_invocation_api import resolve_remote_mapper
from workers.local_hdf_reducer import launch_worker as launch_local_hdf_reducer
from common import distribution_metric

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
LAUNCH_NAME = "remote_local_hdf"

def get_default_value_for_metrics_dict():
    return {}

def launch_test(
    how_many_samples: int=None,
    how_many_mappers: int=None,
    faas_environment: RemoteEnvironment=None,
    **_rest_of_args
) -> Dict:
    """
    A function that runs a given test case.
    A test case is described with the arguments listed below.

    Args:
        how_many_samples (int): number of samples that should be generated
        how_many_mappers (int): number of workers that should be used for samples generation
        faas_environment (RemoteEnvironment): "whisk" if HPCWHisk should be used, "aws" if AWS Lambda should be used

    Returns:
        Dict: a dictionary with metrics gathered within the test
    """

    # initial preparation
    metrics = initialize_metrics()
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    os.makedirs(FINAL_RESULTS, exist_ok=True)
    launch_single_mapper = resolve_remote_mapper(faas_environment)
    launch_multiple_mappers = prepare_multiple_remote_mappers_function(
        launch_single_mapper
    )

    # mapping
    start_time = time.time()
    dat_files = FilesystemBinary(INPUT_FILES_DIR, transform=lzma.compress).to_memory()
    (
        in_memory_mapper_results,
        map_time,
        mappers_request_times,
        mappers_simulation_times,
    ) = launch_multiple_mappers(
        how_many_samples,
        how_many_mappers,
        dat_files,
        True,
        save_to="download",
    )
    mapper_filesystem_hdf_results = in_memory_mapper_results.to_filesystem(
        TEMPORARY_RESULTS
    ).to_hdf()
    # reducing
    reducer_in_memory_results, reduce_time = launch_local_hdf_reducer(
        mapper_filesystem_hdf_results
    )
    reducer_in_memory_results.to_filesystem(FINAL_RESULTS)
    total_duration = time.time() - start_time
    # update metrics
    metrics["phases"] = ["simulating_and_extracting", "reducing"]
    
    metrics["number_of_workers"]["simulate_and_extract"] = how_many_mappers
    metrics["number_of_workers"]["reduce"] = 1
    
    metrics["workers_request_times"]["simulate_and_extract"] = mappers_request_times
    metrics["workers_request_times"]["reduce"] = [reduce_time]

    metrics["workers_execution_times"]["simulate_and_extract"] = mappers_simulation_times
    metrics["workers_execution_times"]["reduce"] = [reduce_time]

    metrics["makespan"]["simulating_and_extracting"] = map_time
    metrics["makespan"]["reducing"] = reduce_time
    metrics["makespan"]["total"] = total_duration

    in_memory_final_results = FilesystemHDF(FINAL_RESULTS).to_memory()
    if "z_profile.h5" in in_memory_final_results.files_map.keys():
        metrics["hdf_results"] = in_memory_final_results.read("z_profile.h5")
        metrics["mse"], metrics["how_many_results_not_delivered"] = distribution_metric(FINAL_RESULTS)
    else:
        metrics["mse"], metrics["how_many_results_not_delivered"] = 1, how_many_mappers

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)
    return dict(metrics)
