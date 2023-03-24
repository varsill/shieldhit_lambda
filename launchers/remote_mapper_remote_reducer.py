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
from workers.common.remote_mapper_invocation_api import (
    RemoteMapperEnvironment,
    resolve_remote_mapper,
)
from workers.common.remote_reducer_invocation_api import (
    RemoteReducerEnvironment,
    resolve_remote_reducer,
)


INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
SHOULD_MAPPER_PRODUCE_HDF = False
OPERATION = "hdf"


def launch_test(
    how_many_samples: int,
    how_many_mappers: int,
    max_samples_per_mapper: int,
    faas_environment: RemoteMapperEnvironment,
) -> Dict:
    """
    A function that runs a given test case.
    A test case is described with the arguments listed below.

    Args:
        how_many_samples (int): number of samples that should be generated
        how_many_mappers (int): number of workers that should be used for samples generation
        max_samples_per_mapper (int):
        faas_environment (RemoteMapperEnvironment): "whisk" if HPCWHisk should be used, "aws" if AWS Lambda should be used

    Returns:
        Dict: a dictionary with metrics gathered within the test
    """

    # initial preparation
    metrics = {}
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    os.makedirs(FINAL_RESULTS, exist_ok=True)
    launch_mapper = resolve_remote_mapper(faas_environment)
    launch_reducer = resolve_remote_reducer(faas_environment)

    # mapping
    dat_files = FilesystemBinary(INPUT_FILES_DIR, transform=lzma.compress).to_memory()
    in_memory_mapper_results, map_time, workers_times = launch_mapper(
        how_many_samples,
        how_many_mappers,
        dat_files,
        SHOULD_MAPPER_PRODUCE_HDF,
    )
    # reducing
    reducer_in_memory_results, cumulative_reduce_time = launch_reducer(
        in_memory_mapper_results, "hdf"
    )
    reducer_in_memory_results.to_filesystem(FINAL_RESULTS)
    # update metrics
    metrics["hdf_results"] = reducer_in_memory_results.read("z_profile_.h5")
    metrics["reduce_time"] = cumulative_reduce_time
    metrics["map_time"] = map_time
    metrics["workers_times"] = workers_times

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return metrics
