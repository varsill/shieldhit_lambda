from launchers.remote_mapper_remote_partial_bdo_reducer_local_hdf_reducer_with_persistent_storage import launch_test, LAUNCH_NAME
import itertools
import pickle
import shutil
import traceback
import subprocess

METRICS_RESULT_BASE_PATH = "/home/ubuntu/backup/lambda_results/new"
FAAS_ENVIRONMENT = "aws"
TEST_RUNNER_POSTFIX=f"{FAAS_ENVIRONMENT}_100-300_workers_1mln_samples"
HOW_MANY_TRIES = 3

def prepare_test_cases(params_dict):
    params_dict_with_enumerable_values = {key: (value if (isinstance(value, range) or isinstance(value, list)) else [value]) for key, value in params_dict.items()}
    results_list = []
    for combination in itertools.product(*list(params_dict_with_enumerable_values.values())):
        new_result = {param_name: param_value for param_name, param_value in zip(params_dict_with_enumerable_values.keys(), combination)}
        results_list.append(new_result)
    return results_list
        
TEST_CASES = {"how_many_mappers": [100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300], "how_many_samples": 1000000, "reduce_when": 1, "faas_environment": FAAS_ENVIRONMENT}

if __name__ == "__main__":
    filename = f"{LAUNCH_NAME}_{TEST_RUNNER_POSTFIX}"
    print(f"{METRICS_RESULT_BASE_PATH}/{filename}.dump")
    test_results = []
    for test_case_params in prepare_test_cases(TEST_CASES):
        for try_number in range(HOW_MANY_TRIES):
            try:
                metrics = launch_test(**test_case_params)

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
        with open(f"{METRICS_RESULT_BASE_PATH}/{filename}.dump", "wb") as output_file:
            pickle.dump(test_results, output_file)
    except Exception as e:
        print("EMERGENCY DUMP", e)
        with open(f"emergency.dump", "wb") as output_file:
            pickle.dump(test_results, output_file)
        subprocess.check_output(f"sudo mv emergency.dump {METRICS_RESULT_BASE_PATH}/{filename}.dump", shell=True)
