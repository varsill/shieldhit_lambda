from launchers.remote_mapper_remote_bdo_reducer import launch_test, LAUNCH_NAME
import pickle
import shutil
import subprocess
import traceback
import itertools

TEST_RUNNER_POSTFIX="10_workers_1000_samples"
METRICS_RESULT_BASE_PATH = "/home/ubuntu/backup/lambda_results"
FAAS_ENVIRONMENT = "aws"
HOW_MANY_TRIES = 3

def prepare_test_cases(params_dict):
    params_dict_with_enumerable_values = {key: (value if (isinstance(value, range) or isinstance(value, list)) else [value]) for key, value in params_dict.items()}
    results_list = []
    for combination in itertools.product(*list(params_dict_with_enumerable_values.values())):
        new_result = {param_name: param_value for param_name, param_value in zip(params_dict_with_enumerable_values.keys(), combination)}
        results_list.append(new_result)
    return results_list
        
TEST_CASES = {"how_many_mappers": range(1, 10), "how_many_samples": 1000, "reduce_when": 1, "faas_environment": FAAS_ENVIRONMENT}

if __name__ == "__main__":
    filename = f"{LAUNCH_NAME}_{FAAS_ENVIRONMENT}_{TEST_RUNNER_POSTFIX}"
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
        filename = f"{LAUNCH_NAME}_{FAAS_ENVIRONMENT}_{TEST_RUNNER_POSTFIX}"
        with open(f"{METRICS_RESULT_BASE_PATH}/{filename}.dump", "wb") as output_file:
            pickle.dump(test_results, output_file)
    except Exception as e:
        print("EMERGENCY DUMP", e)
        with open(f"emergency.dump", "wb") as output_file:
            pickle.dump(test_results, output_file)
        subprocess.check_output(f"sudo mv emergency.dump {METRICS_RESULT_BASE_PATH}/{LAUNCH_NAME}_{FAAS_ENVIRONMENT}_{TEST_RUNNER_POSTFIX}.dump", shell=True)
