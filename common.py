import subprocess
from multiprocessing import Pool
import time
from typing import Callable, Tuple, TypeVar
import os
import glob
import shutil

REF_RESULTS="/home/ubuntu/shieldhit_lambda/ref_results"

T = TypeVar("T")


def cmd(command: str):
    """
    Executes a command in the shell

    Args:
        command (str): the command to be executed
    """
    return subprocess.check_output(command.split())


def meassure_time(f: Callable[[], T]) -> Tuple[T, float]:
    """
    Function that meassures the time of function execution and
    returns that time along with the function result

    Args:
        f (Callable[[], T]): function accepting 0 args and
          returning value type T, that will be invoked

    Returns:
        (T, float): a tuple consisting of a function invocation
        result and a duration of the function execution
    """
    start_time = time.time()
    result = f()
    end_time = time.time()
    return result, end_time - start_time


def execute_concurrently(function, N):
    if N == 1:
        return [function(0)]
    else:
        with Pool(N) as process:
            results = process.map_async(function, range(N))
            results.wait()
        return results.get()


def load_hdf_result_file(file_path):
    import h5py
    import numpy as np

    if not os.path.isfile(file_path):
        return None
    f = h5py.File(file_path, "r")
    if "data" in f.keys():
        return np.array(f["data"])
    else:
        results_list = []
        for key in f.keys():
            if key.startswith("data_"):
                results_list.append(np.array(f[key]))
        return results_list



def separate_results(input_dir, output_dir):
    for filename in glob.glob(f"{input_dir}/*"):
        _directory_path, just_file_name = os.path.split(filename)
        result_name, _, _worker_id = just_file_name.rpartition("_")
        result_subdir = f"{output_dir}/{result_name}"
        os.makedirs(result_subdir, exist_ok=True)
        if os.path.exists(os.path.join(result_subdir, filename)):
            os.remove(os.path.join(result_subdir, filename))
        shutil.move(filename, result_subdir)


def mktemp(dir=""):
    if dir == "":
        return subprocess.check_output(["mktemp", "-d"]).decode()[:-1]
    else:
        return subprocess.check_output(["mktemp", "-d", "-p", dir]).decode()[:-1]

def distribution_metric(results_dir, reference_results_dir=REF_RESULTS):
    import numpy as np

    results_filenames = _get_all_files_matching_the_regex(results_dir, "*.h5")
    ref_results_filenames = _get_all_files_matching_the_regex(reference_results_dir, "*.h5")
    common_filenames = list(set(results_filenames) & set(ref_results_filenames))
    how_many_results_not_produced = len(ref_results_filenames)-len(common_filenames)

    if len(results_filenames)==0:
        return 1, how_many_results_not_produced
    
    cumulative_mse = 0
    for filename in common_filenames:
        results = load_hdf_result_file(f"{results_dir}/{filename}")
        ref_results = load_hdf_result_file(f"{reference_results_dir}/{filename}")
        if isinstance(results, list):
            for page, ref_page in zip(results, ref_results):
                mse = np.sum((page - ref_page) ** 2)
                mse_normalized = mse / np.sum(ref_page**2)
                cumulative_mse += mse_normalized
        else:
            mse = np.sum((results - ref_results) ** 2)
            mse_normalized = mse / np.sum(ref_results**2)
            cumulative_mse += mse_normalized
    return cumulative_mse, how_many_results_not_produced


def _get_all_files_matching_the_regex(directory_path, regex_str):
    all_result_filenames = []
    for path in glob.glob(f"{directory_path}/{regex_str}"):
        _dir_path, just_filename = os.path.split(path)
        all_result_filenames.append(just_filename)
    return all_result_filenames