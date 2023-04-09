import lzma
import os
import shutil
from typing import Dict
import time

from datatypes.filesystem import FilesystemBinary, FilesystemHDF
from launchers.common import prepare_multiple_remote_mappers_function, initialize_metrics
from workers.common.remote import RemoteEnvironment
from workers.common.remote_mapper_invocation_api import resolve_remote_mapper
from workers.common.remote_reducer_invocation_api import resolve_remote_reducer
from common import distribution_metric

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
LAUNCH_NAME = "remote_remote_bdo"

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
    launch_reducer = resolve_remote_reducer(faas_environment)

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
        False,
        save_to="download",
    )
    # reducing
    (
        reducer_in_memory_results,
        reducer_simulation_time,
        reducer_request_time,
    ) = launch_reducer(in_memory_mapper_results, "hdf", get_from="uploaded")

    reducer_in_memory_results.to_filesystem(FINAL_RESULTS)
    total_duration = time.time()-start_time
    # update metrics

    metrics["phases"] = ["simulating", "extracting_and_reducing"]
    
    metrics["number_of_workers"]["simulate"] = how_many_mappers
    metrics["number_of_workers"]["extract_and_reduce"] = 1
    
    metrics["workers_request_times"]["simulate"] = mappers_request_times
    metrics["workers_request_times"]["extract_and_reduce"] = [reducer_request_time]

    metrics["workers_execution_times"]["simulate"] = mappers_simulation_times
    metrics["workers_execution_times"]["extract_and_reduce"] = [reducer_simulation_time]

    metrics["makespan"]["simulating"] = map_time
    metrics["makespan"]["extracting_and_reducing"] = reducer_request_time
    metrics["makespan"]["total"] = total_duration

    metrics["hdf_results"] = FilesystemHDF(FINAL_RESULTS).to_memory().read("z_profile_.h5")
    metrics["mse"], metrics["how_many_results_not_delivered"] = distribution_metric(FINAL_RESULTS)
    print(metrics["mse"], metrics["how_many_results_not_delivered"])
    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return dict(metrics)
