import functools
import os
import shutil
import subprocess
from typing import Dict
import time
from collections import defaultdict

from common import execute_concurrently, meassure_time
from datatypes.filesystem import FilesystemHDF
from workers.common.remote import RemoteEnvironment

INPUT_FILES_DIR = "input"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
OPERATION = "hdf"
LAUNCH_NAME = "local_local"


def launch_mapper(mapper_id, how_many_samples_per_mapper):
    _mapping_result, worker_time = meassure_time(
        lambda: subprocess.check_output(
            f"./binaries/shieldhit -n {how_many_samples_per_mapper} -N {mapper_id} {TEMPORARY_RESULTS}",
            shell=True,
        )
    )
    return worker_time

def get_default_value_for_metrics_dict():
    return {}

def launch_test(
    how_many_samples: int,
    how_many_mappers: int,
    faas_environment: RemoteEnvironment,
) -> Dict:
    # initial preparation
    metrics = defaultdict(get_default_value_for_metrics_dict)
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
            f"./binaries/convertmc {OPERATION} --many '{TEMPORARY_RESULTS}/*.bdo' {TEMPORARY_RESULTS}",
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

    metrics["hdf_results"] = FilesystemHDF(FINAL_RESULTS).to_memory().read("z_profile_.h5")

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return dict(metrics)
