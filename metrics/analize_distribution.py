import pickle
import pandas as pd
import matplotlib.pyplot as plt
import h5py
import numpy as np

RESULTS_DUMP_FILE = "metrics/results/aws_large.dump"
PLOT_FILE = "DISTRIBUTION.png"

def __load_hdf(file_path):
  f = h5py.File(file_path, 'r')
  return np.array(f['data'])

def __load_dump(input_file_path):
  with open(input_file_path, "rb") as input_file:
    results = pickle.load(input_file)
  return results

results_ref = __load_hdf("metrics/z_profile_ref.h5")

results_dump = __load_dump(RESULTS_DUMP_FILE)
results_dump = pd.json_normalize(results_dump).filter(axis="columns", items=["hdf_results", "params.number_of_workers"])

indices_to_replace = []
for i, value in enumerate(np.array(results_dump['hdf_results'].values)):
  if value is None:
    indices_to_replace.append(i)

zeroes = np.zeros(results_ref.shape)
results_dump['hdf_results'].update(pd.Series([zeroes]*len(indices_to_replace), index=indices_to_replace))

results_dump_avg = results_dump.groupby("params.number_of_workers").agg(
    lambda x: np.average(x)
).reset_index()

X = []
Y = []

for (_index, value) in results_dump_avg.iterrows():
  single_results = value["hdf_results"]
  mse = np.sum((single_results-results_ref)**2)
  mse_normalized = mse/np.sum(results_ref**2)
  X.append(value["params.number_of_workers"])
  Y.append(mse_normalized)

plt.scatter(X, Y)
plt.xlabel("Number of workers")
plt.ylabel("MSE in contrast to single worker results")
plt.yscale("log")
plt.savefig(PLOT_FILE)