from common import meassure_time
import subprocess
import random

def simulate(id, how_many_samples, results_dir):
    _result, simulate_time = meassure_time(
        lambda: subprocess.check_output(
            f"./binaries/shieldhit -n {how_many_samples} -N {random.randint(1, 30)} {results_dir}",
            shell=True,
        )
    )
    return simulate_time
