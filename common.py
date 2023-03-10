import subprocess
import time
from typing import Callable, Tuple, TypeVar

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
