import lzma
import os
import shutil
from typing import Dict

from datatypes.filesystem import FilesystemBinary
from launchers.common import prepare_multiple_remote_mappers_function
from workers.common.remote import RemoteEnvironment
from workers.common.remote_mapper_invocation_api import resolve_remote_mapper
from workers.local_bdo_reducer import launch_worker as launch_local_bdo_reducer

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
SHOULD_MAPPER_PRODUCE_HDF = False
OPERATION = "hdf"
LAUNCH_NAME = "remote_local_bdo"


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
    (
        in_memory_mapper_results,
        map_time,
        mappers_request_times,
        mappers_simulation_times,
    ) = launch_multiple_mappers(
        how_many_samples,
        how_many_mappers,
        dat_files,
        SHOULD_MAPPER_PRODUCE_HDF,
        save_to="download",
    )
    mapper_filesystem_binary_results = in_memory_mapper_results.to_filesystem(
        TEMPORARY_RESULTS
    )
    # reducing
    reducer_filesystem_result, reduce_time = launch_local_bdo_reducer(
        mapper_filesystem_binary_results, FINAL_RESULTS, "hdf"
    )

    # update metrics
    try:
        metrics["hdf_results"] = reducer_filesystem_result.to_memory().read(
            "z_profile_.h5"
        )
    except Exception:
        pass
    metrics["map_time"] = map_time
    metrics["mappers_request_times"] = mappers_request_times
    metrics["mappers_simulation_times"] = mappers_simulation_times
    metrics["reduce_time"] = reduce_time

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    return metrics
