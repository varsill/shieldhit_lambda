from launchers.whisk_mapper_local_bdo_reducer import launch_test
from common import meassure_time
import pickle
import shutil
import numpy as np
import os

METRICS_RESULT_PATH = "metrics/results/aws_test5.dump"

HOW_MANY_TRIES = 1

TEST_CASES = [
    {"number_of_workers": 100, "number_of_samples": 1000000},
    {"number_of_workers": 150, "number_of_samples": 1000000},
    {"number_of_workers": 200, "number_of_samples": 1000000},
    {"number_of_workers": 250, "number_of_samples": 1000000},
    {"number_of_workers": 300, "number_of_samples": 1000000},
    {"number_of_workers": 350, "number_of_samples": 1000000},
    {"number_of_workers": 400, "number_of_samples": 1000000},
    {"number_of_workers": 800, "number_of_samples": 1000000},
    {"number_of_workers": 900, "number_of_samples": 1000000},
    {"number_of_workers": 1000, "number_of_samples": 1000000},
    # {"number_of_workers": 11, "number_of_samples": 10000},
    # {"number_of_workers": 12, "number_of_samples": 10000},
    # {"number_of_workers": 13, "number_of_samples": 10000},
    # {"number_of_workers": 14, "number_of_samples": 10000},
    # {"number_of_workers": 15, "number_of_samples": 10000},
    # {"number_of_workers": 16, "number_of_samples": 10000},
    # {"number_of_workers": 17, "number_of_samples": 10000},
    # {"number_of_workers": 18, "number_of_samples": 10000}, 
    # {"number_of_workers": 19, "number_of_samples": 10000},
    # {"number_of_workers": 20, "number_of_samples": 10000},
    # {"number_of_workers": 21, "number_of_samples": 10000},
    # {"number_of_workers": 22, "number_of_samples": 10000},
    # {"number_of_workers": 23, "number_of_samples": 10000},
    # {"number_of_workers": 24, "number_of_samples": 10000},
    # {"number_of_workers": 25, "number_of_samples": 10000},
    # {"number_of_workers": 30, "number_of_samples": 10000},
    # {"number_of_workers": 40, "number_of_samples": 10000},
    # {"number_of_workers": 60, "number_of_samples": 100000},
    # {"number_of_workers": 80, "number_of_samples": 10000},
    # {"number_of_workers": 100, "number_of_samples": 10000},
]
TEST_CASES.reverse()

if __name__ == "__main__":
    test_results = []
    for test_case_params in TEST_CASES:
        for try_number in range(HOW_MANY_TRIES):
            metrics, duration = meassure_time(
                lambda: launch_test(
                    how_many_samples=test_case_params["number_of_samples"],
                    how_many_workers=test_case_params["number_of_workers"]
                )
            )
            metrics["total_duration"] = duration
            
            test_instance = {
                "params": test_case_params,
                "metrics": metrics,
                "test_run_number": try_number
            }
            
            test_results.append(test_instance)

    with open(METRICS_RESULT_PATH, "wb") as output_file:
        pickle.dump(test_results, output_file)
