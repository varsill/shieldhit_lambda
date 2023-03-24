import subprocess
from multiprocessing import Pool
import time
from typing import Callable, Tuple, TypeVar
import os
import glob
import shutil
from converters import Converters
import lzma

SHOULD_USE_MEMFD = False

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
        return [function()]
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
    return np.array(f["data"])


def separate_results(input_dir, output_dir):
    for filename in glob.glob(f"{input_dir}/*"):
        _directory_path, just_file_name = os.path.split(filename)
        result_name, _, _worker_id = just_file_name.rpartition("_")
        result_subdir = f"{output_dir}/{result_name}"
        os.makedirs(result_subdir, exist_ok=True)
        if os.path.exists(os.path.join(result_subdir, filename)):
            os.remove(os.path.join(result_subdir, filename))
        shutil.move(filename, result_subdir)

def mktemp():
    return subprocess.check_output(["mktemp", "-d"]).decode()[:-1]
