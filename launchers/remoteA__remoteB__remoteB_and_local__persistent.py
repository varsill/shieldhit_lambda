import functools
import glob
import lzma
import multiprocessing
import os
import shutil
import time
from multiprocessing import Manager, Pool
from typing import Dict

from common import mse, psnr, separate_results
from datatypes.filesystem import FilesystemBinary, FilesystemHDF
from datatypes.in_memory import InMemoryBinary
from launchers.common import initialize_metrics
from workers.common.remote import RemoteEnvironment
from workers.common.remote_invocation_api import resolve_remote_function
from workers.local.reduce import reduce

INPUT_FILES_DIR = "input/"
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"
WHISK_VOLUME = "/net/people/plgvarsill/persistent_volume"


def _resolve_persistent_storage(faas_environment):
    if faas_environment == "aws":
        return "s3", "s3"
    elif faas_environment == "whisk":
        return ("volume", WHISK_VOLUME), "volume"
    else:
        raise Exception(f"Unknown FaaS environment: {faas_environment}")


def simulate_extract_and_partially_reduce(
    worker_id,
    lock,
    launch_single_simulate,
    launch_extract_and_reduce,
    how_many_samples,
    dat_files,
    results_map,
    tmp_dir,
    save_to,
    get_from,
    reduce_when,
):
    try:
        result = launch_single_simulate(
            worker_id, how_many_samples, dat_files, save_to=save_to
        )
        number_of_files_produced_by_me = len(result["files"].read_all().keys())

        with lock:
            if (
                int(
                    (len(results_map.keys()) + number_of_files_produced_by_me)
                    / number_of_files_produced_by_me
                )
                == reduce_when
            ):
                left_in_memory_results_map = {}
                for key, value in results_map.items():
                    left_in_memory_results_map[key] = value
                left_in_memory_simulate_results = InMemoryBinary(
                    left_in_memory_results_map, transform=lzma.compress
                )
                for key in results_map.keys():
                    del results_map[key]
            else:
                for key, value in result["files"].read_all().items():
                    results_map[key] = value
                return {
                    "status": "OK",
                    "type": "JUST_SIMULATE",
                    "simulate_request_time": result["request_time"],
                    "simulate_execution_time": result["execution_time"],
                }
        (
            in_memory_extract_and_reduce_results,
            extract_and_reduce_execution_time,
            extract_and_reduce_request_time,
        ) = launch_extract_and_reduce(
            left_in_memory_simulate_results,
            get_from=get_from,
            worker_id_prefix=str(worker_id),
        )

        filesystem_extract_and_reduce_results = (
            in_memory_extract_and_reduce_results.to_filesystem(tmp_dir)
        )
        return {
            "status": "OK",
            "type": "SIMULATE_EXTRACT_REDUCE",
            "results": filesystem_extract_and_reduce_results,
            "simulate_request_time": result["request_time"],
            "simulate_execution_time": result["execution_time"],
            "extract_and_reduce_request_time": extract_and_reduce_request_time,
            "extract_and_reduce_execution_time": extract_and_reduce_execution_time,
        }
    except Exception as e:
        print(e)
        return {"status": "ERROR"}


def get_default_value_for_metrics_dict():
    return {}


def launch_test(
    how_many_samples: int = None,
    how_many_workers: int = None,
    faas_environment: RemoteEnvironment = None,
    reduce_when: int = None,
    **_rest_of_args,
) -> Dict:
    """
    A function that runs a given test case.
    A test case is described with the arguments listed below.

    Args:
        how_many_samples (int): number of samples that should be generated
        how_many_workers (int): number of workers that should be used for samples generation
        faas_environment (RemoteEnvironment): "whisk" if HPCWHisk should be used, "aws" if AWS Lambda should be used

    Returns:
        Dict: a dictionary with metrics gathered within the test
    """

    # initial preparation
    metrics = initialize_metrics()
    os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
    os.makedirs(FINAL_RESULTS, exist_ok=True)
    launch_single_simulate = resolve_remote_function("simulate", faas_environment)
    launch_extract_and_reduce = resolve_remote_function(
        "extract_and_reduce", faas_environment
    )
    how_many_samples_per_simulate_worker = int(how_many_samples / how_many_workers)
    save_to, get_from = _resolve_persistent_storage(faas_environment)
    # simulate, extract and partial reduce
    start_time = time.time()
    dat_files = FilesystemBinary(INPUT_FILES_DIR, transform=lzma.compress).to_memory()
    m = multiprocessing.Manager()
    lock = m.Lock()
    manager = Manager()
    shared_results_map = manager.dict()

    with Pool(processes=how_many_workers) as pool:
        concurrent_function = functools.partial(
            simulate_extract_and_partially_reduce,
            lock=lock,
            launch_single_simulate=launch_single_simulate,
            launch_extract_and_reduce=launch_extract_and_reduce,
            how_many_samples=how_many_samples_per_simulate_worker,
            dat_files=dat_files,
            results_map=shared_results_map,
            tmp_dir=TEMPORARY_RESULTS,
            save_to=save_to,
            get_from=get_from,
            reduce_when=reduce_when,
        )
        results = pool.map_async(concurrent_function, range(how_many_workers))
        results.wait()

    results = results.get()
    simulate_extract_and_partially_reduce_time = time.time() - start_time
    number_of_all_results = len(results)
    ok_results = [r for r in results if r["status"] == "OK"]
    number_of_ok_results = len(ok_results)
    print(f"SUCCESS/ALL: {number_of_ok_results}/{number_of_all_results}")

    separate_results(TEMPORARY_RESULTS, TEMPORARY_RESULTS)

    # final reduce
    cumulative_final_reduce_time = 0
    for subdir in glob.glob(f"{TEMPORARY_RESULTS}/*"):
        partially_reduced_results = FilesystemBinary(subdir, "*.h5")
        in_memory_results, final_reduce_time = reduce(partially_reduced_results)
        cumulative_final_reduce_time += final_reduce_time
        in_memory_results.to_filesystem(FINAL_RESULTS)

    total_duration = time.time() - start_time
    # update metrics
    metrics["phases"] = [
        "simulating_extracting_and_partially_reducing",
        "final_reducing",
    ]

    metrics["number_of_workers"]["simulate"] = how_many_workers
    metrics["number_of_workers"]["extract_and_partially_reduce"] = int(
        how_many_workers / reduce_when
    )
    metrics["number_of_workers"]["final_reduce"] = 1

    metrics["workers_request_times"]["simulate"] = [
        r["simulate_request_time"] for r in ok_results
    ]
    metrics["workers_request_times"]["extract_and_partially_reduce"] = [
        r["extract_and_reduce_request_time"]
        for r in ok_results
        if r["type"] == "MAP_EXTRACT_REDUCE"
    ]
    metrics["workers_request_times"]["final_reduce"] = [cumulative_final_reduce_time]

    metrics["workers_execution_times"]["simulate"] = [
        r["simulate_execution_time"] for r in ok_results
    ]
    metrics["workers_execution_times"]["extract_and_partially_reduce"] = [
        r["extract_and_reduce_execution_time"]
        for r in ok_results
        if r["type"] == "MAP_EXTRACT_REDUCE"
    ]
    metrics["workers_execution_times"]["final_reduce"] = [cumulative_final_reduce_time]

    metrics["makespan"][
        "simulating_extracting_and_partially_reducing"
    ] = simulate_extract_and_partially_reduce_time
    metrics["makespan"]["final_reducing"] = cumulative_final_reduce_time
    metrics["makespan"]["total"] = total_duration

    in_memory_final_results = FilesystemHDF(FINAL_RESULTS).to_memory()
    if "z_profile.h5" in in_memory_final_results.files_map.keys():
        print("THERE IS NO OUTPUT FILE!")
        #metrics["hdf_results"] = in_memory_final_results.read("z_profile.h5")
    metrics["mse"] = mse(FINAL_RESULTS)
    metrics["psnr"] = psnr(FINAL_RESULTS)

    # cleanup
    shutil.rmtree(TEMPORARY_RESULTS)
    shutil.rmtree(FINAL_RESULTS)
    clear()

    return dict(metrics)

def clear():
    import requests
    lambda_url = "https://172.17.0.1:10001/api/v1/web/guest/default/shieldHit"
    json_input = {"action": "clear", "dir": WHISK_VOLUME}
    requests.post(lambda_url, json=json_input, verify=False)
  