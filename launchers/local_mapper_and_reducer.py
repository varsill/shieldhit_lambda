import subprocess
from datatypes.filesystem import FilesystemHDF
from workers.common.remote_mapper_invocation_api import RemoteMapperEnvironment
from typing import Dict
import os
from common import meassure_time, execute_concurrently
import functools
import shutil

INPUT_FILES_DIR = "input"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
OPERATION = "hdf"

def launch_mapper(mapper_id, how_many_samples_per_mapper):
  _mapping_result, worker_time = meassure_time(lambda: subprocess.check_output(f"./binaries/shieldhit -n {how_many_samples_per_mapper} -N {mapper_id} {TEMPORARY_RESULTS}", shell=True))
  return worker_time

def launch_test(
    how_many_samples: int,
    how_many_mappers: int,
    max_samples_per_mapper: int,
    faas_environment: RemoteMapperEnvironment,
) -> Dict:
    # initial preparation
    metrics = {}
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    os.makedirs(FINAL_RESULTS, exist_ok=True)
    how_many_samples_per_mapper = int(how_many_samples/how_many_mappers)
    # mapping
    subprocess.check_output(f"cp {INPUT_FILES_DIR}/* {TEMPORARY_RESULTS}", shell=True)
    workers_times, map_time = meassure_time(lambda: execute_concurrently(functools.partial(launch_mapper, how_many_samples_per_mapper=how_many_samples_per_mapper), how_many_mappers))
    # reducing
    _reducer_results, reduce_time = meassure_time(lambda: subprocess.check_output(f"./binaries/convertmc {OPERATION} --many '{TEMPORARY_RESULTS}/*.bdo' {TEMPORARY_RESULTS}", shell=True))
    subprocess.check_output(f"cp {TEMPORARY_RESULTS}/*.h5 {FINAL_RESULTS}", shell=True)
    # update metrics
    reducer_filesystem_result = FilesystemHDF(FINAL_RESULTS)
    metrics["hdf_results"] = reducer_filesystem_result.to_memory().read("z_profile_.h5")
    metrics["reduce_time"] = reduce_time
    metrics["map_time"] = map_time
    metrics["workers_times"] = workers_times
    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)
    
    return metrics