import pickle
import pandas as pd
import matplotlib.pyplot as plt
import functools
import subprocess

RESULTS_BASE_PATH = "/home/ubuntu/backup/lambda_results"
FILENAME = "remote_local_bdo_whisk_60-200_workers_100k_samples"
RESULTS_DUMP_FILE = f"{RESULTS_BASE_PATH}/{FILENAME}.dump"
GROUP_BY_PARAM = "number_of_workers"
# to_plot = results.filter(axis="columns", items=["test_run_number", "params.number_of_workers"])
# print(results.iloc[0])
# results_std = results.groupby("params.number_of_workers").std(
#     ["results.map_time", "results.reduce_time", "results.total_duration"]
# )

def _agregate_times(column, metric): 
    if column.name != f"metrics.{metric}":
        return None
    else:
        result = []
        for row in column:
            result = result+row 
        return result


def plot_request_times_histogram(results, group_by_param, plot_filename, metric="mappers_simulation_times", group_by_param_value=100):
    results = pd.json_normalize(results)
    results = results[results[f"params.{group_by_param}"]==group_by_param_value]
    agregator = functools.partial(_agregate_times, metric=metric)
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .agg(agregator)
        .reset_index()
    )
    all_request_times = results_avg[f"metrics.{metric}"].tolist()[0]
    plt.hist(all_request_times)
    plt.xlabel("Request time [s]")
    plt.ylabel(f"Number of occurences")
    plt.savefig(plot_filename)


def plot_simulation_time_to_request_time_vs_param(results, group_by_param, plot_filename, workers="mappers"):
    results = pd.json_normalize(results)
    #results = results[results[f"metrics.{workers}_simulation_times"]!=[] and results[f"metrics.{workers}_request_times"]!=[]]
    results["mean_simulation_time"] = results.apply(lambda row: sum(row[f"metrics.{workers}_simulation_times"])/len(row[f"metrics.{workers}_simulation_times"]) if len(row[f"metrics.{workers}_simulation_times"])>0 else 0, axis=1)
    results["mean_request_time"] = results.apply(lambda row: sum(row[f"metrics.{workers}_request_times"])/len(row[f"metrics.{workers}_request_times"]) if len(row[f"metrics.{workers}_request_times"])>0 else 0, axis=1)
    results["simulation_to_request"] = results.apply(lambda row: row["mean_simulation_time"]/row["mean_request_time"] if row["mean_request_time"]>0 else 0, axis=1)
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["simulation_to_request", "mean_simulation_time", "mean_request_time"])
        .reset_index()
    )
    plt.bar(results_avg[f"params.{group_by_param}"] , results_avg["mean_request_time"], 5, label = 'Request time')
    plt.bar(results_avg[f"params.{group_by_param}"], results_avg["mean_simulation_time"], 5, label = 'Simulation time')
    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"Time [s]")
    plt.title(f"Simulation and request time for {workers}")
    plt.savefig(plot_filename)

def plot_percentage_of_successfull_responses(results, group_by_param, plot_filename):
    results = pd.json_normalize(results)
    results["successfull_responses_perc"] = results.apply(lambda row: len(row["metrics.mappers_request_times"])/row["params.number_of_workers"], axis=1)
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["successfull_responses_perc"])
        .reset_index()
    )
    plt.errorbar(
        results_avg[f"params.{group_by_param}"],
        results_avg["successfull_responses_perc"],
        label="Successfull mapper responses percentage",
    )
    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"% of successfull responses")
    plt.savefig(plot_filename)

def plot_simulation_time_and_request_time_ratio_vs_param(results, group_by_param, plot_filename, workers="mappers"):
    results = pd.json_normalize(results)
    results["mean_simulation_time"] = results.apply(lambda row: sum(row[f"metrics.{workers}_simulation_times"])/len(row[f"metrics.{workers}_simulation_times"]) if len(row[f"metrics.{workers}_simulation_times"])>0 else 0, axis=1)
    results["mean_request_time"] = results.apply(lambda row: sum(row[f"metrics.{workers}_request_times"])/len(row[f"metrics.{workers}_request_times"]) if len(row[f"metrics.{workers}_request_times"])>0 else 0, axis=1)
    results["simulation_to_request"] = results.apply(lambda row: row["mean_simulation_time"]/row["mean_request_time"]*100 if row["mean_request_time"]>0 else 0, axis=1)
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["simulation_to_request"])
        .reset_index()
    )
    plt.errorbar(
        results_avg[f"params.{group_by_param}"],
        results_avg["simulation_to_request"]
    )
    plt.legend()
    plt.title(f"Simulation time to request time ratio for {workers}")
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"[%]")
    plt.savefig(plot_filename)



def plot_map_reduce_total_time_vs_params(results, group_by_param, plot_filename):
    results = pd.json_normalize(results)
    #results = results.drop(columns=["metrics.mappers_times", "test_run_number"])

    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["metrics.map_time", "metrics.reduce_time", "metrics.total_duration"])
        .reset_index()
    )

    results_std = results.groupby(f"params.{group_by_param}").std().fillna(0)

    x = results_avg[f"params.{group_by_param}"]
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
    plt.xlabel(f"{group_by_param}")
    plt.ylabel("Time [s]")
    plt.savefig(plot_filename)



#plot_percentage_of_successfull_responses(results, GROUP_BY_PARAM, f"{FILENAME}_time_vs_{GROUP_BY_PARAM}.png")
#plot_request_times_histogram(results, GROUP_BY_PARAM, f"{FILENAME}_simulation_and_request_time_vs{GROUP_BY_PARAM}.png")