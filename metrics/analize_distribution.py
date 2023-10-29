import h5py
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def __load_hdf(file_path):
    f = h5py.File(file_path, "r")
    return np.array(f["data"])


def plot_distribution(results_dump, group_by_param, plot_filename, title):
    results_ref = __load_hdf("z_profile_ref.h5")

    results_dump = results_dump.filter(
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
    plt.xlabel(group_by_param)
    plt.ylabel("MSE in contrast to single worker results")
    plt.yscale("log")
    plt.ylim([0, 0.0001])
    plt.title(title)
    plt.savefig(plot_filename)


def plot_mse(results, group_by_param):
    results_avg = results.groupby(f"params.{group_by_param}").mean().reset_index()
    results_std = results.groupby(f"params.{group_by_param}").std().fillna(0)
    plt.errorbar(
        results_avg[f"params.{group_by_param}"],
        results_avg["metrics.mse"]/1750,
        results_std["metrics.mse"]/1750,
        linestyle="None",
        marker=".",
        label="Meassured values"
    )
    plt.plot(results_avg[f"params.{group_by_param}"], np.sqrt(10)/np.sqrt(results_avg[f"params.{group_by_param}"]), '--', label=r"$y=\frac{1}{\sqrt{x}}$")

def plot_psnr(results, group_by_param, label):
    results_avg = results.groupby(f"params.{group_by_param}").mean().reset_index()
    results_std = results.groupby(f"params.{group_by_param}").std().fillna(0)
    plt.errorbar(
        results_avg[f"params.{group_by_param}"],
        results_avg["metrics.psnr"],
        results_std["metrics.psnr"],
        linestyle="None",
        marker=".",
        label=label
    )
    plt.plot(results_avg[f"params.{group_by_param}"], -np.sqrt(results_avg[f"params.{group_by_param}"]))
    plt.xlabel(group_by_param)
    plt.ylabel("PSNR in contrast to single worker results")
    plt.yscale("log")
    plt.ylim([0, 0.0001])
