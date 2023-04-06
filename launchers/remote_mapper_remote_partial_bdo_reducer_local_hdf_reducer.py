import functools
import glob
import lzma
import multiprocessing
import os
import shutil
import time
from multiprocessing import Pool
from typing import Dict

from common import meassure_time, separate_results
from datatypes.filesystem import FilesystemBinary
from workers.common.remote import RemoteEnvironment
from workers.common.remote_mapper_invocation_api import resolve_remote_mapper
from workers.common.remote_reducer_invocation_api import resolve_remote_reducer
from workers.local_hdf_reducer import launch_worker as launch_local_hdf_reducer

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
SHOULD_MAPPER_PRODUCE_HDF = False
OPERATION = "hdf"
REDUCE_WHEN = 5
LAUNCH_NAME = f"remote_remote_bdo_{REDUCE_WHEN}_local_hdf"


def mapper_and_bdo_reducer(
    worker_id,
    lock,
    launch_single_mapper,
    launch_reducer,
    how_many_samples,
    dat_files,
    should_produce_hdf,
    tmp_dir,
    reduce_when,
):
    try:
        result = launch_single_mapper(
            worker_id,
            how_many_samples,
            dat_files,
            should_produce_hdf,
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
                    "type": "JUST_MAPPER",
                    "mapper_request_time": result["request_time"],
                    "mapper_simulation_time": result["simulation_time"],
                }
        (
            reducer_in_memory_results,
            reduce_time_simulation,
            reduce_time_request,
        ) = launch_reducer(
            left_in_memory_mapper_results,
            "hdf",
            worker_id_prefix=str(worker_id),
            get_from="uploaded",
        )
        reducer_in_filesystem_results = reducer_in_memory_results.to_filesystem(tmp_dir)
        return {
            "status": "OK",
            "type": "MAPPER_AND_REDUCER",
            "results": reducer_in_filesystem_results,
            "mapper_request_time": result["request_time"],
            "mapper_simulation_time": result["simulation_time"],
            "bdo_reducer_request_time": reduce_time_request,
            "bdo_reducer_simulation_time": reduce_time_simulation,
        }
    except Exception:
        return {"status": "ERROR"}


def launch_test(
    how_many_samples: int,
    how_many_mappers: int,
    faas_environment: RemoteEnvironment,
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
    metrics = {}
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    os.makedirs(FINAL_RESULTS, exist_ok=True)
    launch_single_mapper = resolve_remote_mapper(faas_environment)
    launch_reducer = resolve_remote_reducer(faas_environment)
    how_many_samples_per_mapper = int(how_many_samples / how_many_mappers)

    # mapping and partial reducing
    dat_files = FilesystemBinary(INPUT_FILES_DIR, transform=lzma.compress).to_memory()
    m = multiprocessing.Manager()
    lock = m.Lock()
    start_time = time.time()
    with Pool(processes=how_many_mappers) as pool:
        concurrent_function = functools.partial(
            mapper_and_bdo_reducer,
            lock=lock,
            launch_single_mapper=launch_single_mapper,
            launch_reducer=launch_reducer,
            how_many_samples=how_many_samples_per_mapper,
            dat_files=dat_files,
            should_produce_hdf=SHOULD_MAPPER_PRODUCE_HDF,
            tmp_dir=TEMPORARY_RESULTS,
            reduce_when=REDUCE_WHEN,
        )
        results = pool.map_async(concurrent_function, range(how_many_mappers))
        results.wait()

    results = results.get()
    mapper_and_bdo_reducer_time = time.time() - start_time
    number_of_all_results = len(results)
    results = [r for r in results if r["status"] == "OK"]
    number_of_ok_results = len(results)
    print(f"SUCCESS/ALL: {number_of_ok_results}/{number_of_all_results}")
    # mapper_and_reducer_in_filesystem_results = FilesystemBinary(
    #     TEMPORARY_RESULTS, "*.h5"
    # )
    separate_results(TEMPORARY_RESULTS, TEMPORARY_RESULTS)
    cumulative_hdf_reduce_time = 0
    for subdir in glob.glob(f"{TEMPORARY_RESULTS}/*"):
        mapper_and_reducer_in_filesystem_results = FilesystemBinary(subdir, "*.h5")
        reducer_in_memory_results, hdf_reducer_time = launch_local_hdf_reducer(
            mapper_and_reducer_in_filesystem_results
        )
        cumulative_hdf_reduce_time += hdf_reducer_time
        if "z_profile.h5" in reducer_in_memory_results.files_map:
            metrics["hdf_results"] = reducer_in_memory_results.read("z_profile.h5")
        reducer_in_memory_results.to_filesystem(FINAL_RESULTS)

    # update metrics
    metrics["hdf_reduce_time"] = cumulative_hdf_reduce_time
    metrics["mapper_and_bdo_reducer_time"] = mapper_and_bdo_reducer_time
    metrics["mappers_request_times"] = [r["mapper_request_time"] for r in results]
    metrics["mappers_simulation_times"] = [r["mapper_simulation_time"] for r in results]
    metrics["bdo_reducers_request_times"] = [
        r["bdo_reducer_request_time"]
        for r in results
        if r["type"] == "MAPPER_AND_REDUCER"
    ]
    metrics["bdo_reducers_simulation_times"] = [
        r["bdo_reducer_simulation_time"]
        for r in results
        if r["type"] == "MAPPER_AND_REDUCER"
    ]
    metrics["map_time"] = None
    metrics["reduce_time"] = None
    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return metrics
