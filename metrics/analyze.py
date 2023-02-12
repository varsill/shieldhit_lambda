import pickle
import pandas as pd
import matplotlib.pyplot as plt

RESULTS_DUMP_FILE = "metrics/results/10000_samples_no_compression.dump"

def load(input_file_path):
  with open(input_file_path, "rb") as input_file:
    results = pickle.load(input_file)    
  return results

def plot(results):
  results = pd.json_normalize(results)
  results = results.drop(columns=["results.mappers_time", "params.number_of_samples", "test_run_number"])
  #to_plot = results.filter(axis="columns", items=["test_run_number", "params.number_of_workers"])
  #print(results.iloc[0])
  results = results.groupby("params.number_of_workers").mean(["results.full_map_time", "results.reduce_time", "results.total_duration"])

  plt.plot(results["results.full_map_time"], label="Map time")
  plt.plot(results["results.reduce_time"], label="Reduce time")
  plt.plot(results["results.total_duration"], label="Full time")
  plt.legend()
  plt.xlabel("Number of workers")
  plt.ylabel("Time [s]")
  plt.show()

results = load(RESULTS_DUMP_FILE)
plot(results)