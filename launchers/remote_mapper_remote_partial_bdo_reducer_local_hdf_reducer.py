import os
import shutil
from common import (
    meassure_time,
    execute_concurrently,
    load_hdf_result_file,
    separate_results,
)
from typing import Dict
import h5py
import lzma
import functools
from common import meassure_time
from converters import Converters
import glob
from datatypes.filesystem import FilesystemBinary
from datatypes.in_memory import InMemoryHDF
from workers.common.remote_mapper_invocation_api import (
    resolve_remote_mapper,
)
from workers.common.remote import RemoteEnvironment
from workers.common.remote_reducer_invocation_api import (
    resolve_remote_reducer,
)
from launchers.common import prepare_multiple_remote_mappers_function
from workers.local_hdf_reducer import launch_worker as launch_local_hdf_reducer
from multiprocessing import Pool, Lock
import multiprocessing

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
SHOULD_MAPPER_PRODUCE_HDF = False
OPERATION = "hdf"
REDUCE_WHEN = 5


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
    result = launch_single_mapper(
        worker_id, how_many_samples, dat_files, should_produce_hdf, save_to="download"
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
                "type": "JUST_MAPPER",
                "map_time": result["request_time"],
                "simulation_time": result["simulation_time"],
            }
    reducer_in_memory_results, reduce_time = launch_reducer(
        left_in_memory_mapper_results,
        "hdf",
        worker_id_prefix=str(worker_id),
        get_from="uploaded",
    )
    reducer_in_filesystem_results = reducer_in_memory_results.to_filesystem(tmp_dir)
    return {
        "type": "MAPPER_AND_REDUCER",
        "results": reducer_in_filesystem_results,
        "map_time": result["request_time"],
        "reduce_time": reduce_time,
        "simulation_time": result["simulation_time"],
    }


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
    map_time = sum([r["map_time"] for r in results])
    bdo_reducer_times = sum(
        r["reduce_time"] for r in results if r["type"] == "MAPPER_AND_REDUCER"
    )
    mapper_and_reducer_in_filesystem_results = FilesystemBinary(
        TEMPORARY_RESULTS, "*.h5"
    )

    reducer_in_memory_results, hdf_reduce_time = launch_local_hdf_reducer(
        mapper_and_reducer_in_filesystem_results
    )
    mappers_times = [r["simulation_time"] for r in results]
    # update metrics
    metrics["hdf_results"] = reducer_in_memory_results.read("z_profile.h5")
    metrics["reduce_time"] = bdo_reducer_times + hdf_reduce_time
    metrics["map_time"] = map_time
    metrics["mappers_times"] = mappers_times

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return metrics
