from launchers.remote_mapper_local_hdf_reducer import launch_test, LAUNCH_NAME
from common import meassure_time
import pickle
import shutil
import numpy as np
import os
import subprocess
import traceback

TEST_RUNNER_POSTFIX="10_workers_1000_samples"
METRICS_RESULT_BASE_PATH = "/home/ubuntu/backup/lambda_results"
FAAS_ENVIRONMENT = "aws"
HOW_MANY_TRIES = 3

TEST_CASES = [
    {"number_of_workers": 10, "number_of_samples": 1000},
    # {"number_of_workers": 70, "number_of_samples": 1000000},
    # {"number_of_workers": 80, "number_of_samples": 1000000},
    # {"number_of_workers": 90, "number_of_samples": 1000000},
    # {"number_of_workers": 100, "number_of_samples": 1000000},
    # {"number_of_workers": 120, "number_of_samples": 1000000},
    # {"number_of_workers": 140, "number_of_samples": 1000000},
    # {"number_of_workers": 160, "number_of_samples": 1000000},
    # {"number_of_workers": 180, "number_of_samples": 1000000},
    # {"number_of_workers": 200, "number_of_samples": 1000000},
    # {"number_of_workers": 230, "number_of_samples": 1000000},
    # {"number_of_workers": 260, "number_of_samples": 1000000},
    # {"number_of_workers": 300, "number_of_samples": 1000000},
    # {"number_of_workers": 330, "number_of_samples": 1000000},
    # {"number_of_workers": 360, "number_of_samples": 1000000},
    # {"number_of_workers": 260, "number_of_samples": 1000000},
    # {"number_of_workers": 280, "number_of_samples": 1000000},
    # {"number_of_workers": 300, "number_of_samples": 1000000},
    # {"number_of_workers": 320, "number_of_samples": 1000000},
    # {"number_of_workers": 340, "number_of_samples": 1000000},
    # {"number_of_workers": 360, "number_of_samples": 1000000},
]
# TEST_CASES.reverse()

if __name__ == "__main__":
    filename = f"{LAUNCH_NAME}_{FAAS_ENVIRONMENT}_{TEST_RUNNER_POSTFIX}"
    print(f"{METRICS_RESULT_BASE_PATH}/{filename}.dump")
    test_results = []
    for test_case_params in TEST_CASES:
        for try_number in range(HOW_MANY_TRIES):
            try:
                metrics, duration = meassure_time(
                    lambda: launch_test(
                        how_many_samples=test_case_params["number_of_samples"],
                        how_many_mappers=test_case_params["number_of_workers"],
                        faas_environment=FAAS_ENVIRONMENT,
                    )
                )
                metrics["total_duration"] = duration
                print("total_duration", metrics["total_duration"])
                print("map_time", metrics["map_time"])
                print("reduce_time", metrics["reduce_time"])
                test_instance = {
                    "params": test_case_params,
                    "metrics": metrics,
                    "test_run_number": try_number,
                }

                test_results.append(test_instance)
            except Exception as e:
                print(traceback.format_exc())
                print("test case runner ERROR", e)
                shutil.rmtree("results/final")
                shutil.rmtree("results/temporary")
                continue
    try:
        filename = f"{LAUNCH_NAME}_{FAAS_ENVIRONMENT}_{TEST_RUNNER_POSTFIX}"
        with open(f"{METRICS_RESULT_BASE_PATH}/{filename}.dump", "wb") as output_file:
            pickle.dump(test_results, output_file)
    except Exception as e:
        print("EMERGENCY DUMP", e)
        with open(f"emergency.dump", "wb") as output_file:
            pickle.dump(test_results, output_file)
        subprocess.check_output(f"sudo mv emergency.dump {METRICS_RESULT_BASE_PATH}/{LAUNCH_NAME}_{FAAS_ENVIRONMENT}_{TEST_RUNNER_POSTFIX}.dump", shell=True)
