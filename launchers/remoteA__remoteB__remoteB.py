import lzma
import os
import shutil
import time
from typing import Dict

from common import mse, psnr
from datatypes.filesystem import FilesystemBinary, FilesystemHDF
from launchers.common import initialize_metrics, prepare_multiple_simulate_functions
from workers.common.remote import RemoteEnvironment
from workers.common.remote_invocation_api import resolve_remote_function

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
LAUNCH_NAME = "remote_remote_bdo"


def get_default_value_for_metrics_dict():
    return {}


def launch_test(
    how_many_samples: int = None,
    how_many_workers: int = None,
    faas_environment: RemoteEnvironment = None,
    **_rest_of_args
) -> Dict:
    """
    A function that runs a given test case.
    A test case is described with the arguments listed below.

    Args:
        how_many_samples (int): number of samples that should be generated
        how_many_workers (int): number of workers that should be used for samples generation
        faas_environment (RemoteEnvironment): "whisk" if HPCWHisk should be used, "aws" if AWS Lambda should be used

    Returns:
        Dict: a dictionary with metrics gathered within the test
    """

    # initial preparation
    metrics = initialize_metrics()
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    os.makedirs(FINAL_RESULTS, exist_ok=True)
    launch_single_simulate = resolve_remote_function("simulate", faas_environment)
    launch_multiple_simulate = prepare_multiple_simulate_functions(
        launch_single_simulate
    )
    launch_reducer = resolve_remote_function("extract_and_reduce", faas_environment)

    # simulate
    start_time = time.time()
    dat_files = FilesystemBinary(INPUT_FILES_DIR, transform=lzma.compress).to_memory()
    (
        in_memory_mapper_results,
        simulate_time,
        simulate_request_times,
        simulate_execution_times,
    ) = launch_multiple_simulate(
        how_many_samples,
        how_many_workers,
        dat_files,
        save_to="download",
    )
    # extract and reduce
    extract_and_reduce_start_time = time.time()
    (
        in_memory_extract_and_reduce_results,
        extract_and_reduce_execution_time,
        extract_and_reduce_request_time,
    ) = launch_reducer(in_memory_mapper_results, get_from="uploaded")

    in_memory_extract_and_reduce_results.to_filesystem(FINAL_RESULTS)
    final_time = time.time()
    extract_and_reduce_time = final_time - extract_and_reduce_start_time
    total_duration = final_time - start_time
    # update metrics

    metrics["phases"] = ["simulating", "extracting_and_reducing"]

    metrics["number_of_workers"]["simulate"] = how_many_workers
    metrics["number_of_workers"]["extract_and_reduce"] = 1

    metrics["workers_request_times"]["simulate"] = simulate_request_times
    metrics["workers_request_times"]["extract_and_reduce"] = [
        extract_and_reduce_request_time
    ]

    metrics["workers_execution_times"]["simulate"] = simulate_execution_times
    metrics["workers_execution_times"]["extract_and_reduce"] = [
        extract_and_reduce_execution_time
    ]

    metrics["makespan"]["simulating"] = simulate_time
    metrics["makespan"]["extracting_and_reducing"] = extract_and_reduce_time
    metrics["makespan"]["total"] = total_duration

    in_memory_final_results = FilesystemHDF(FINAL_RESULTS).to_memory()
    if "z_profile_.h5" in in_memory_final_results.files_map.keys():
        print("THERE IS NO OUTPUT FILE!")
        #metrics["hdf_results"] = in_memory_final_results.read("z_profile_.h5")
    metrics["mse"] = mse(FINAL_RESULTS)
    metrics["psnr"] = psnr(FINAL_RESULTS)
    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return dict(metrics)
