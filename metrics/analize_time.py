import pickle
import pandas as pd
import matplotlib.pyplot as plt

RESULTS_DUMP_FILE = "metrics/results/just_test.dump"
PLOT_FILE = "TIME.png"
GROUP_BY_PARAM = "number_of_samples"
# to_plot = results.filter(axis="columns", items=["test_run_number", "params.number_of_workers"])
# print(results.iloc[0])
# results_std = results.groupby("params.number_of_workers").std(
#     ["results.map_time", "results.reduce_time", "results.total_duration"]
# )


def load(input_file_path):
    with open(input_file_path, "rb") as input_file:
        results = pickle.load(input_file)
    return results


def plot(results):
    results = pd.json_normalize(results)
    results = results.drop(
        columns=["metrics.workers_times", "test_run_number"]
    )

    results_avg = (
        results.groupby(f"params.{GROUP_BY_PARAM}")
        .mean(["metrics.map_time", "metrics.reduce_time", "metrics.total_duration"])
        .reset_index()
    )

    results_std = results.groupby(f"params.{GROUP_BY_PARAM}").std().fillna(0)

    x = results_avg[f"params.{GROUP_BY_PARAM}"]
    plt.errorbar(
        x,
        results_avg["metrics.map_time"],
        results_std["metrics.map_time"],
        label="Map time",
    )
    plt.errorbar(
        x,
        results_avg["metrics.reduce_time"],
        results_std["metrics.reduce_time"],
        label="Reduce time",
    )
    plt.errorbar(
        x,
        results_avg["metrics.total_duration"],
        results_std["metrics.total_duration"],
        label="Full time",
    )
    plt.legend()
    plt.xlabel(f"{GROUP_BY_PARAM}")
    plt.ylabel("Time [s]")
    plt.savefig(PLOT_FILE)


results = load(RESULTS_DUMP_FILE)
plot(results)
