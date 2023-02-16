import os
import shutil
from common import meassure_time
from environments.mapper import Mapper
from environments.reducer import Reducer
from typing import Dict

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"


def run(
    how_many_samples: int,
    how_many_workers: int,
    mapper_module: Mapper,
    reducer_module: Reducer,
) -> Dict:
    """
    A function that runs a given test case.
    A test case is described with the arguments listed below.

    Args:
        how_many_samples (int): number of samples that should be generated
        how_many_workers (int): number of workers that should be used for samples generation
        mapper_module (Mapper): the module delivering the logic of sampling generation
        reducer_module (Reducer): the module delivering the logic of samples gathering

    Returns:
        Dict: a dictionary with metrics gathered within the test
    """
    mapper = mapper_module(
        how_many_samples, how_many_workers, INPUT_FILES_DIR, TEMPORARY_RESULTS
    )
    reducer = reducer_module(TEMPORARY_RESULTS, FINAL_RESULTS)

    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)

    metrics, map_time = meassure_time(lambda: mapper.execute())

    os.makedirs(FINAL_RESULTS, exist_ok=True)
    _, reduce_time = meassure_time(lambda: reducer.execute())
    shutil.rmtree(TEMPORARY_RESULTS)

    metrics["map_time"] = map_time
    metrics["reduce_time"] = reduce_time
    return metrics
