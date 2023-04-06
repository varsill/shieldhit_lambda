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


def plot_request_times_histogram(results, group_by_param, plot_filename, metric, group_by_param_value, title):
    results = results[results[f"params.{group_by_param}"]==group_by_param_value]
    agregator = functools.partial(_agregate_times, metric=metric)
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .agg(agregator)
        .reset_index()
    )
    print(results_avg)
    all_request_times = results_avg[f"metrics.{metric}"].tolist()[0]
    plt.hist(all_request_times)
    plt.xlabel("Request time [s]")
    plt.ylabel(f"Number of occurences")
    plt.title(title)
    plt.savefig(plot_filename)


def plot_simulation_time_to_request_time_vs_param(results, group_by_param, plot_filename, workers, title):
    plt.tight_layout(pad=2)
    results["mean_simulation_time"] = results.apply(lambda row: sum(row[f"metrics.{workers}_simulation_times"])/len(row[f"metrics.{workers}_simulation_times"]) if len(row[f"metrics.{workers}_simulation_times"])>0 else 0, axis=1)
    results["mean_request_time"] = results.apply(lambda row: sum(row[f"metrics.{workers}_request_times"])/len(row[f"metrics.{workers}_request_times"]) if len(row[f"metrics.{workers}_request_times"])>0 else 0, axis=1)
    results["simulation_to_request"] = results.apply(lambda row: row["mean_simulation_time"]/row["mean_request_time"] if row["mean_request_time"]>0 else 0, axis=1)
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["simulation_to_request", "mean_simulation_time", "mean_request_time"])
        .reset_index()
    )
    plt.bar(results_avg[f"params.{group_by_param}"] , results_avg["mean_request_time"], 5, label = 'Request time')
    plt.bar(results_avg[f"params.{group_by_param}"], results_avg["mean_simulation_time"], 5, label = 'Execution time')
    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"Time [s]")
    plt.title(title)
    plt.savefig(plot_filename)

def plot_percentage_of_successfull_responses(results, group_by_param, plot_filename, title):
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
    plt.ylim([0, 1.1])
    plt.title(title)
    plt.savefig(plot_filename)

# def plot_simulation_time_and_request_time_ratio_vs_param(results, group_by_param, plot_filename, workers, title):
#     results["mean_simulation_time"] = results.apply(lambda row: sum(row[f"metrics.{workers}_simulation_times"])/len(row[f"metrics.{workers}_simulation_times"]) if len(row[f"metrics.{workers}_simulation_times"])>0 else 0, axis=1)
#     results["mean_request_time"] = results.apply(lambda row: sum(row[f"metrics.{workers}_request_times"])/len(row[f"metrics.{workers}_request_times"]) if len(row[f"metrics.{workers}_request_times"])>0 else 0, axis=1)
#     results["simulation_to_request"] = results.apply(lambda row: row["mean_simulation_time"]/row["mean_request_time"]*100 if row["mean_request_time"]>0 else 0, axis=1)
#     results_avg = (
#         results.groupby(f"params.{group_by_param}")
#         .mean(["simulation_to_request"])
#         .reset_index()
#     )
#     plt.errorbar(
#         results_avg[f"params.{group_by_param}"],
#         results_avg["simulation_to_request"]
#     )
#     plt.legend()
#     plt.title(f"Simulation time to request time ratio for {workers}")
#     plt.xlabel(f"{group_by_param}")
#     plt.ylabel(f"[%]")
#     plt.title(title)
#     plt.savefig(plot_filename)


def plot_map_reduce_total_time_vs_params(results, group_by_param, plot_filename, times_fields_list, title):
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(times_fields_list)
        .reset_index()
    )

    results_std = results.groupby(f"params.{group_by_param}").std().fillna(0)

    x = results_avg[f"params.{group_by_param}"]
    for field in times_fields_list:
        plt.errorbar(
            x,
            results_avg[f"metrics.{field}"],
            results_std[f"metrics.{field}"],
            label=f"{field} time",
        )

    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel("Time [s]")
    plt.title(title)
    plt.savefig(plot_filename)

def plot_simulation_time_and_request_time_ratio_vs_param(results, group_by_param, plot_filename, workers, title):
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
    #plt.title(f"Simulation time to request time ratio for {workers}")
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"[%]")
    plt.ylim([0,100])
    plt.title(title)
    plt.savefig(plot_filename)



def plot_cumulative_time_vs_params(results, group_by_param, plot_filename, workers, title):
    results["simulation_time_sum"] = results.apply(lambda row: sum(row[f"metrics.{workers}_simulation_times"]), axis=1)
    results["request_time_sum"] = results.apply(lambda row: sum(row[f"metrics.{workers}_request_times"]), axis=1)
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["simulation_time_sum", "request_time_sum"])
        .reset_index()
    )
    results_std = results.groupby(f"params.{group_by_param}").std().fillna(0)

    x = results_avg[f"params.{group_by_param}"]
    plt.errorbar(
        x,
        results_avg[f"simulation_time_sum"],
        results_std[f"simulation_time_sum"],
        label=f"simulation cumulative time",
    )

    plt.errorbar(
        x,
        results_avg[f"request_time_sum"],
        results_std[f"request_time_sum"],
        label=f"request cumulative time",
    )

    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel("Time [s]")
    plt.title(title)
    plt.savefig(plot_filename)

def plot_speedup(input_results_dump, group_by_param, plot_filename, title):
    ONE_PROCESS_TIME = 2000
    input_results_dump["speedup"] = input_results_dump.apply(lambda row: ONE_PROCESS_TIME/(row[f"metrics.total_duration"]-4), axis=1)
    results_avg = (
        input_results_dump.groupby(f"params.{group_by_param}")
        .mean(["speedup"])
        .reset_index()
    )
    x = results_avg[f"params.{group_by_param}"]
    plt.errorbar(
        x,
        results_avg[f"speedup"],
        label=f"Speedup",
    )

    plt.errorbar(
        x,
        x,
        label=f"Ref",
    )

    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel("Speedup")
    plt.title(title)
    plt.savefig(plot_filename)
