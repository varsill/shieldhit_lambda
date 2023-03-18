import os
import shutil
from common import meassure_time, execute_concurrently, load_hdf_result_file
from workers.aws_mapper import launch_worker as launch_mapper
from workers.local_bdo_reducer import launch_worker as launch_reducer
from typing import Dict
import h5py
import lzma
import functools
from common import meassure_time
from converters import Converters
import glob


INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
SHOULD_MAPPER_PRODUCE_HDF = False
SHOULD_USE_MEMFD = False
OPERATION = "hdf"

def launch_test(
    how_many_samples: int,
    how_many_workers: int,
) -> Dict:
    """
    A function that runs a given test case.
    A test case is described with the arguments listed below.

    Args:
        how_many_samples (int): number of samples that should be generated
        how_many_workers (int): number of workers that should be used for samples generation

    Returns:
        Dict: a dictionary with metrics gathered within the test
    """

    metrics = {}
    samples_per_worker = int(how_many_samples / how_many_workers)
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    dat_files = Converters.files_to_map(
            glob.glob(f"{INPUT_FILES_DIR}/*"), lzma.compress
        )
    mapper_function = functools.partial(
        launch_mapper,
        how_many_samples=samples_per_worker,
        files=dat_files,
        should_produce_hdf=SHOULD_MAPPER_PRODUCE_HDF
    )
    
    mapper_results, map_time = meassure_time(lambda:  execute_concurrently(mapper_function, how_many_workers))
    
    successfull_mapper_results = [r for r in mapper_results if r["status"]=="OK"]
    print(f"SUCCESS/ALL: {len(successfull_mapper_results)}/{how_many_workers}")
    
    workers_times = []

    for r in successfull_mapper_results:
        Converters.map_to_files(r["files"], TEMPORARY_RESULTS, lzma.decompress, SHOULD_USE_MEMFD)
        del r["files"]
        workers_times.append(r)
    
    metrics["workers_times"]=workers_times
    
    reducer_function =  functools.partial(
        launch_reducer,
        input_files_dir=TEMPORARY_RESULTS,
        output_dir=FINAL_RESULTS, 
        operation=OPERATION
    )
    os.makedirs(FINAL_RESULTS, exist_ok=True)
  
    _reducer_results, reduce_time = meassure_time(lambda: execute_concurrently(reducer_function, 1))

    metrics["hdf_results"] = load_hdf_result_file(f"{FINAL_RESULTS}/z_profile_.h5")

    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)

    metrics["map_time"] = map_time
    metrics["reduce_time"] = reduce_time

    return metrics
  
