import functools
import os
import shutil
import subprocess
from typing import Dict
import time


from common import execute_concurrently, meassure_time, distribution_metric
from launchers.common import initialize_metrics
from datatypes.filesystem import FilesystemHDF

INPUT_FILES_DIR = "input"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
LAUNCH_NAME = "local_local"


def launch_mapper(mapper_id, how_many_samples_per_mapper, **_rest_of_args):
    _mapping_result, worker_time = meassure_time(
        lambda: subprocess.check_output(
            f"./binaries/shieldhit -n {how_many_samples_per_mapper} -N {mapper_id} {TEMPORARY_RESULTS}",
            shell=True,
        )
    )
    return worker_time



def launch_test(
    how_many_samples: int=None,
    how_many_mappers: int=None,
    **_rest_of_args,
) -> Dict:
    # initial preparation
    metrics = initialize_metrics()
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    os.makedirs(FINAL_RESULTS, exist_ok=True)
    how_many_samples_per_mapper = int(how_many_samples / how_many_mappers)
    # mapping
    start_time = time.time()
    subprocess.check_output(f"cp {INPUT_FILES_DIR}/* {TEMPORARY_RESULTS}", shell=True)
    mappers_times, map_time = meassure_time(
        lambda: execute_concurrently(
            functools.partial(
                launch_mapper, how_many_samples_per_mapper=how_many_samples_per_mapper
            ),
            how_many_mappers,
        )
    )
    # reducing
    _reducer_results, reduce_time = meassure_time(
        lambda: subprocess.check_output(
            f"./binaries/convertmc hdf --many '{TEMPORARY_RESULTS}/*.bdo' {TEMPORARY_RESULTS}",
            shell=True,
        )
    )
    subprocess.check_output(f"cp {TEMPORARY_RESULTS}/*.h5 {FINAL_RESULTS}", shell=True)
    total_duration = time.time() - start_time
    
    # update metrics
    metrics["phases"] = ["simulating", "extracting_reducing"]
    
    metrics["number_of_workers"]["simulate"] = how_many_mappers
    metrics["number_of_workers"]["extract_and_reduce"] = 1
    
    metrics["workers_request_times"]["simulate"] = mappers_times
    metrics["workers_request_times"]["extract_and_reduce"] = [reduce_time]

    metrics["workers_execution_times"]["simulate"] = mappers_times
    metrics["workers_execution_times"]["extract_and_reduce"] = [reduce_time]

    metrics["makespan"]["simulating"] = map_time
    metrics["makespan"]["extracting_and_reducing"] = reduce_time
    metrics["makespan"]["total"] = total_duration

    in_memory_final_results = FilesystemHDF(FINAL_RESULTS).to_memory()
    if "z_profile_.h5" in in_memory_final_results.files_map.keys():
        metrics["hdf_results"] = in_memory_final_results.read("z_profile_.h5")
    metrics["mse"], metrics["how_many_results_not_delivered"] = distribution_metric(FINAL_RESULTS)

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return dict(metrics)
