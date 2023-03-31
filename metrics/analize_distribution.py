import pickle
import pandas as pd
import matplotlib.pyplot as plt
import h5py
import numpy as np

RESULTS_DUMP_FILE = "emergency.dump"
PLOT_FILE = "DISTRIBUTION.png"
GROUP_BY_PARAM = "number_of_workers"


def __load_hdf(file_path):
    f = h5py.File(file_path, "r")
    return np.array(f["data"])



def plot_distribution(results_dump, group_by_param, plot_filename):
    results_ref = __load_hdf("metrics/z_profile_ref.h5")

    results_dump = pd.json_normalize(results_dump).filter(
        axis="columns", items=["metrics.hdf_results", f"params.{group_by_param}"]
    )

    indices_to_replace = []
    for i, value in enumerate(np.array(results_dump["metrics.hdf_results"].values)):
        if value is None:
            indices_to_replace.append(i)

    zeroes = np.zeros(results_ref.shape)
    results_dump["metrics.hdf_results"].update(
        pd.Series([zeroes] * len(indices_to_replace), index=indices_to_replace)
    )

    results_dump_avg = (
        results_dump.groupby(f"params.{group_by_param}")
        .agg(lambda x: np.average(x))
        .reset_index()
    )

    X = []
    Y = []

    for _index, value in results_dump_avg.iterrows():
        single_results = value["metrics.hdf_results"]
        mse = np.sum((single_results - results_ref) ** 2)
        mse_normalized = mse / np.sum(results_ref**2)
        X.append(value[f"params.{group_by_param}"])
        Y.append(mse_normalized)

    plt.scatter(X, Y)
    plt.xlabel("Number of workers")
    plt.ylabel("MSE in contrast to single worker results")
    plt.yscale("log")
    plt.savefig(plot_filename)
