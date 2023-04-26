import functools
import os
import shutil
import subprocess
import time
from typing import Dict

from common import distribution_metric, execute_concurrently, meassure_time
from datatypes.filesystem import FilesystemHDF
from launchers.common import initialize_metrics
from workers.local.simulate import simulate

INPUT_FILES_DIR = "input"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"


def launch_test(
    how_many_samples: int = None,
    how_many_workers: int = None,
    **_rest_of_args,
) -> Dict:
    # initial preparation
    metrics = initialize_metrics()
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    os.makedirs(FINAL_RESULTS, exist_ok=True)
    how_many_samples_per_simulation_worker = int(how_many_samples / how_many_workers)
    # simulate
    start_time = time.time()
    subprocess.check_output(f"cp {INPUT_FILES_DIR}/* {TEMPORARY_RESULTS}", shell=True)
    simulate_workers_times, simulate_time = meassure_time(
        lambda: execute_concurrently(
            functools.partial(
                simulate,
                how_many_samples=how_many_samples_per_simulation_worker,
                results_dir=TEMPORARY_RESULTS,
            ),
            how_many_workers,
        )
    )
    # extract and reduce
    _reducer_results, extract_and_reduce_time = meassure_time(
        lambda: subprocess.check_output(
            f"./binaries/convertmc hdf --many '{TEMPORARY_RESULTS}/*.bdo' {TEMPORARY_RESULTS}",
            shell=True,
        )
    )
    subprocess.check_output(f"cp {TEMPORARY_RESULTS}/*.h5 {FINAL_RESULTS}", shell=True)
    total_duration = time.time() - start_time

    # update metrics
    metrics["phases"] = ["simulating", "extracting_reducing"]

    metrics["number_of_workers"]["simulate"] = how_many_workers
    metrics["number_of_workers"]["extract_and_reduce"] = 1

    metrics["workers_request_times"]["simulate"] = simulate_workers_times
    metrics["workers_request_times"]["extract_and_reduce"] = [extract_and_reduce_time]

    metrics["workers_execution_times"]["simulate"] = simulate_workers_times
    metrics["workers_execution_times"]["extract_and_reduce"] = [extract_and_reduce_time]

    metrics["makespan"]["simulating"] = simulate_time
    metrics["makespan"]["extracting_and_reducing"] = extract_and_reduce_time
    metrics["makespan"]["total"] = total_duration

    in_memory_final_results = FilesystemHDF(FINAL_RESULTS).to_memory()
    if "z_profile_.h5" in in_memory_final_results.files_map.keys():
        metrics["hdf_results"] = in_memory_final_results.read("z_profile_.h5")
    metrics["mse"], metrics["how_many_results_not_delivered"] = distribution_metric(
        FINAL_RESULTS
    )

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return dict(metrics)
