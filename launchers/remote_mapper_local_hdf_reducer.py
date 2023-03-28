import os
import shutil
from common import (
    meassure_time,
    execute_concurrently,
    load_hdf_result_file,
    separate_results,
)
from workers.aws_mapper import launch_worker as launch_mapper
from workers.local_hdf_reducer import launch_worker as launch_local_hdf_reducer
from typing import Dict
import h5py
import lzma
import functools
from common import meassure_time
from converters import Converters
import glob
from datatypes.filesystem import FilesystemBinary, FilesystemHDF
import lzma
from workers.common.remote_mapper_invocation_api import (
    resolve_remote_mapper,
)
from launchers.common import prepare_multiple_remote_mappers_function
from workers.common.remote import RemoteEnvironment

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
SHOULD_MAPPER_PRODUCE_HDF = True
OPERATION = "hdf"


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
    launch_multiple_mappers = prepare_multiple_remote_mappers_function(
        launch_single_mapper
    )
    # mapping
    dat_files = FilesystemBinary(INPUT_FILES_DIR, transform=lzma.compress).to_memory()
    in_memory_mapper_results, map_time, mappers_times = launch_multiple_mappers(
        how_many_samples,
        how_many_mappers,
        dat_files,
        SHOULD_MAPPER_PRODUCE_HDF,
        save_to="download",
    )
    mapper_filesystem_hdf_results = in_memory_mapper_results.to_filesystem(
        TEMPORARY_RESULTS
    ).to_hdf()
    # reducing
    reducer_in_memory_results, cumulative_reduce_time = launch_local_hdf_reducer(
        mapper_filesystem_hdf_results
    )
    reducer_in_memory_results.to_filesystem(FINAL_RESULTS)
    # update metrics
    metrics["hdf_results"] = reducer_in_memory_results.read("z_profile.h5")
    metrics["reduce_time"] = cumulative_reduce_time
    metrics["map_time"] = map_time
    metrics["mappers_times"] = mappers_times

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)
    return metrics
