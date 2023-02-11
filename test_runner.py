from launch import run, meassure_time
import pickle
import shutil

HOW_MANY_TRIES = 3

TEST_CASES = [
  {"number_of_workers": 1, "number_of_samples": 10000},
  {"number_of_workers": 2, "number_of_samples": 10000},
  {"number_of_workers": 5, "number_of_samples": 10000},
  {"number_of_workers": 10, "number_of_samples": 10000}
]

if __name__ == '__main__':
  test_results = []
  for test_case_params in TEST_CASES:
    for try_number in range(HOW_MANY_TRIES):
      metrics, duration = meassure_time(
        lambda: run(
          how_many_samples=test_case_params["number_of_samples"], 
          how_many_workers=test_case_params["number_of_workers"])
        )
      metrics["total_duration"] = duration
      shutil.rmtree("results/final")
      test_instance = {"params":  test_case_params, "results": metrics, "test_run_number": try_number}
      test_results.append(test_instance)
  
  with open("metrics/results/test_results.dump", "wb") as output_file:
    pickle.dump(test_results, output_file)