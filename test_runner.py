LAUNCH_NAME = "remoteA__remoteB__remoteB_and_local__persistent"
#METRICS_RESULT_BASE_PATH = "/home/ubuntu/backup/lambda_results/final"
METRICS_RESULT_BASE_PATH = "/home/ubuntu/shieldhit_lambda/metrics_results"
FAAS_ENVIRONMENT = "aws"
#TEST_RUNNER_POSTFIX=f"{FAAS_ENVIRONMENT}_60-300_workers_1mln_samples_test"
TEST_RUNNER_POSTFIX=""
HOW_MANY_TRIES = 3

import importlib
launcher = importlib.import_module(f"launchers.{LAUNCH_NAME}")
import itertools
import pickle
import shutil
import traceback
import subprocess
from metrics.merge_results import append

def prepare_test_cases(params_dict):
    params_dict_with_enumerable_values = {key: (value if (isinstance(value, range) or isinstance(value, list)) else [value]) for key, value in params_dict.items()}
    results_list = []
    for combination in itertools.product(*list(params_dict_with_enumerable_values.values())):
        new_result = {param_name: param_value for param_name, param_value in zip(params_dict_with_enumerable_values.keys(), combination)}
        results_list.append(new_result)
    return results_list


workers_range = [60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320, 340, 360, 380, 400, 420, 440, 460, 480, 500]
#workers_range = [60, 70, 80, 90]
TEST_CASES = {"how_many_workers": workers_range, "how_many_samples": 1000000, "reduce_when": [2, 3, 5], "faas_environment": FAAS_ENVIRONMENT}
if __name__ == "__main__":
    filename = f"{LAUNCH_NAME}_{TEST_RUNNER_POSTFIX}"
    print(f"{METRICS_RESULT_BASE_PATH}/{filename}.dump")
    test_results = []
    for test_case_params in prepare_test_cases(TEST_CASES):
        for try_number in range(HOW_MANY_TRIES):
            try:
                metrics = launcher.launch_test(**test_case_params)

                test_instance_result = {
                    "params": test_case_params,
                    "metrics": metrics,
                    "test_run_number": try_number,
                }

            except Exception as e:
                print(traceback.format_exc())
                print("test case runner ERROR", e)
                shutil.rmtree("results/final")
                shutil.rmtree("results/temporary")
                continue
            try:
                results_dump_filename = f"{METRICS_RESULT_BASE_PATH}/{filename}.dump"
                append(results_dump_filename, test_instance_result)
            except Exception as e:
                print("EMERGENCY DUMP", e)
                with open(f"emergency.dump", "wb") as output_file:
                    pickle.dump(test_results, output_file)
                subprocess.check_output(f"sudo mv emergency.dump {METRICS_RESULT_BASE_PATH}/{filename}.dump", shell=True)
