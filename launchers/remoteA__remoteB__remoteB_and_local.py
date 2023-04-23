import functools
import glob
import lzma
import multiprocessing
import os
import shutil
import time
from multiprocessing import Pool
from typing import Dict

from common import separate_results, distribution_metric
from datatypes.filesystem import FilesystemBinary, FilesystemHDF
from workers.common.remote import RemoteEnvironment
from workers.common.remote_mapper_invocation_api import resolve_remote_mapper
from workers.common.remote_reducer_invocation_api import resolve_remote_reducer
from workers.local_hdf_reducer import launch_worker as launch_local_hdf_reducer
from launchers.common import initialize_metrics

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
LAUNCH_NAME = f"remote_remote_bdo_local_hdf"


def mapper_and_bdo_reducer(
    worker_id,
    lock,
    launch_single_mapper,
    launch_reducer,
    how_many_samples,
    dat_files,
    tmp_dir,
    reduce_when,
):
    try:
        result = launch_single_mapper(
            worker_id,
            how_many_samples,
            dat_files,
            save_to="download",
        )
        number_of_files_produced_by_me = len(result["files"].read_all().keys())
        with lock:
            result["files"].to_filesystem(tmp_dir)
            number_of_files_in_tmpdir = len(
                [
                    name
                    for name in os.listdir(tmp_dir)
                    if os.path.isfile(os.path.join(tmp_dir, name))
                ]
            )
            if (
                int(number_of_files_in_tmpdir / number_of_files_produced_by_me)
                == reduce_when
            ):
                left_in_memory_mapper_results = FilesystemBinary(
                    tmp_dir, "*", lzma.compress
                ).to_memory()
                for f in glob.glob(f"{tmp_dir}/*"):
                    os.remove(f)
            else:
                return {
                    "status": "OK",
                    "type": "JUST_SIMULATE",
                    "mapper_request_time": result["request_time"],
                    "mapper_simulation_time": result["simulation_time"],
                }
        (
            reducer_in_memory_results,
            reduce_time_simulation,
            reduce_time_request,
        ) = launch_reducer(
            left_in_memory_mapper_results,
            worker_id_prefix=str(worker_id),
            get_from="uploaded",
        )
        reducer_in_filesystem_results = reducer_in_memory_results.to_filesystem(tmp_dir)
        return {
            "status": "OK",
            "type": "MAP_EXTRACT_REDUCE",
            "results": reducer_in_filesystem_results,
            "mapper_request_time": result["request_time"],
            "mapper_simulation_time": result["simulation_time"],
            "bdo_reducer_request_time": reduce_time_request,
            "bdo_reducer_simulation_time": reduce_time_simulation,
        }
    except Exception:
        return {"status": "ERROR"}

def get_default_value_for_metrics_dict():
    return {}

def launch_test(
    how_many_samples: int=None,
    how_many_mappers: int=None,
    faas_environment: RemoteEnvironment=None,
    reduce_when: int=None,
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
    launch_reducer = resolve_remote_reducer(faas_environment)
    how_many_samples_per_mapper = int(how_many_samples / how_many_mappers)

    # mapping and partial reducing
    start_time = time.time()
    dat_files = FilesystemBinary(INPUT_FILES_DIR, transform=lzma.compress).to_memory()
    m = multiprocessing.Manager()
    lock = m.Lock()
    
    with Pool(processes=how_many_mappers) as pool:
        concurrent_function = functools.partial(
            mapper_and_bdo_reducer,
            lock=lock,
            launch_single_mapper=launch_single_mapper,
            launch_reducer=launch_reducer,
            how_many_samples=how_many_samples_per_mapper,
            dat_files=dat_files,
            tmp_dir=TEMPORARY_RESULTS,
            reduce_when=reduce_when,
        )
        results = pool.map_async(concurrent_function, range(how_many_mappers))
        results.wait()

    results = results.get()
    mapper_and_bdo_reducer_time = time.time() - start_time
    number_of_all_results = len(results)
    results = [r for r in results if r["status"] == "OK"]
    number_of_ok_results = len(results)
    print(f"SUCCESS/ALL: {number_of_ok_results}/{number_of_all_results}")

    separate_results(TEMPORARY_RESULTS, TEMPORARY_RESULTS)
    cumulative_hdf_reduce_time = 0
    for subdir in glob.glob(f"{TEMPORARY_RESULTS}/*"):
        mapper_and_reducer_in_filesystem_results = FilesystemBinary(subdir, "*.h5")
        reducer_in_memory_results, hdf_reducer_time = launch_local_hdf_reducer(
            mapper_and_reducer_in_filesystem_results
        )
        cumulative_hdf_reduce_time += hdf_reducer_time
        reducer_in_memory_results.to_filesystem(FINAL_RESULTS)

    total_duration = time.time()-start_time

    # update metrics
    metrics["phases"] = ["simulating_extracting_and_partially_reducing", "final_reducing"]
    
    metrics["number_of_workers"]["simulate"] = how_many_mappers
    metrics["number_of_workers"]["extract_and_partially_reduce"] = int(how_many_mappers/reduce_when)
    metrics["number_of_workers"]["final_reduce"] = 1
    
    metrics["workers_request_times"]["simulate"] = [r["mapper_request_time"] for r in results]
    metrics["workers_request_times"]["extract_and_partially_reduce"] = [r["bdo_reducer_request_time"] for r in results if r["type"] == "MAP_EXTRACT_REDUCE"]
    metrics["workers_request_times"]["final_reduce"] = [cumulative_hdf_reduce_time]

    metrics["workers_execution_times"]["simulate"] = [r["mapper_simulation_time"] for r in results]
    metrics["workers_execution_times"]["extract_and_partially_reduce"] = [r["bdo_reducer_simulation_time"] for r in results if r["type"] == "MAP_EXTRACT_REDUCE"]
    metrics["workers_execution_times"]["final_reduce"] = [cumulative_hdf_reduce_time]
    
    metrics["makespan"]["simulating_extracting_and_partially_reducing"] = mapper_and_bdo_reducer_time
    metrics["makespan"]["final_reducing"] = cumulative_hdf_reduce_time
    metrics["makespan"]["total"] = total_duration

    in_memory_final_results = FilesystemHDF(FINAL_RESULTS).to_memory()
    if "z_profile.h5" in in_memory_final_results.files_map.keys():
        metrics["hdf_results"] = in_memory_final_results.read("z_profile.h5")
    metrics["mse"], metrics["how_many_results_not_delivered"] = distribution_metric(FINAL_RESULTS)

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return dict(metrics)
