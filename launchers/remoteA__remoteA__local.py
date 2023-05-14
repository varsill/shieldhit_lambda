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
from workers.local.reduce import reduce

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"


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
    launch_single_simulate_and_extract = resolve_remote_function(
        "simulate_and_extract", faas_environment
    )
    launch_multiple_simulate_and_extract = prepare_multiple_simulate_functions(
        launch_single_simulate_and_extract
    )

    # simulate and extract
    start_time = time.time()
    dat_files = FilesystemBinary(INPUT_FILES_DIR, transform=lzma.compress).to_memory()
    (
        in_memory_simulate_and_extract_results,
        simulate_and_extract_time,
        simulate_and_extract_request_times,
        simulate_and_extract_execution_times,
    ) = launch_multiple_simulate_and_extract(
        how_many_samples,
        how_many_workers,
        dat_files,
        save_to="download",
    )
    filesystem_simulate_and_extract_results = (
        in_memory_simulate_and_extract_results.to_filesystem(TEMPORARY_RESULTS).to_hdf()
    )
    # reduce
    reduce_in_memory_results, reduce_time = reduce(
        filesystem_simulate_and_extract_results
    )
    reduce_in_memory_results.to_filesystem(FINAL_RESULTS)
    total_duration = time.time() - start_time
    # update metrics
    metrics["phases"] = ["simulating_and_extracting", "reducing"]

    metrics["number_of_workers"]["simulate_and_extract"] = how_many_workers
    metrics["number_of_workers"]["reduce"] = 1

    metrics["workers_request_times"][
        "simulate_and_extract"
    ] = simulate_and_extract_request_times
    metrics["workers_request_times"]["reduce"] = [reduce_time]

    metrics["workers_execution_times"][
        "simulate_and_extract"
    ] = simulate_and_extract_execution_times
    metrics["workers_execution_times"]["reduce"] = [reduce_time]

    metrics["makespan"]["simulating_and_extracting"] = simulate_and_extract_time
    metrics["makespan"]["reducing"] = reduce_time
    metrics["makespan"]["total"] = total_duration

    in_memory_final_results = FilesystemHDF(FINAL_RESULTS).to_memory()
    if "z_profile.h5" not in in_memory_final_results.files_map.keys():
        print("THERE IS NO OUTPUT FILE!")
        #metrics["hdf_results"] = in_memory_final_results.read("z_profile.h5")
    metrics["mse"] = mse(FINAL_RESULTS)
    metrics["psnr"] = psnr(FINAL_RESULTS)

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)
    return dict(metrics)
