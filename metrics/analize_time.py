import functools

import matplotlib.pyplot as plt

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
            result = result + row
        return result


def plot_request_times_histogram(
    results, group_by_param, plot_filename, metric, group_by_param_value, title
):
    results = results[results[f"params.{group_by_param}"] == group_by_param_value]
    agregator = functools.partial(_agregate_times, metric=metric)
    results_avg = (
        results.groupby(f"params.{group_by_param}").agg(agregator).reset_index()
    )
    
    all_request_times = results_avg[f"metrics.{metric}"].tolist()[0]
    plt.hist(all_request_times)
    plt.xlabel("Request time [s]")
    plt.ylabel(f"Number of occurences")
    plt.title(title)
    plt.savefig(plot_filename)


def plot_execution_time_to_request_time_vs_param(
    results, group_by_param, plot_filename, worker, title
):
    plt.tight_layout(pad=2)
    results["mean_execution_time"] = results.apply(
        lambda row: sum(row[f"metrics.workers_execution_times.{worker}"])
        / len(row[f"metrics.workers_execution_times.{worker}"])
        if len(row[f"metrics.workers_execution_times.{worker}"]) > 0
        else 0,
        axis=1,
    )
    results["mean_request_time"] = results.apply(
        lambda row: sum(row[f"metrics.workers_request_times.{worker}"])
        / len(row[f"metrics.workers_request_times.{worker}"])
        if len(row[f"metrics.workers_request_times.{worker}"]) > 0
        else 0,
        axis=1,
    )
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["mean_execution_time", "mean_request_time"])
        .reset_index()
    )
    plt.bar(
        results_avg[f"params.{group_by_param}"],
        results_avg["mean_request_time"],
        label="Request time"
    )
    plt.bar(
        results_avg[f"params.{group_by_param}"],
        results_avg["mean_execution_time"],
        label="Execution time"
    )
    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"Time [s]")
    plt.title(title)
    plt.savefig(plot_filename)


def plot_percentage_of_successfull_responses(
    results, group_by_param, plot_filename, worker, title
):
    results["successfull_responses_perc"] = results.apply(
        lambda row: len(row[f"metrics.workers_request_times.{worker}"])
        / row[f"metrics.number_of_workers.{worker}"],
        axis=1,
    )
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


def plot_phases_makespan_vs_params(
    results, group_by_param, plot_filename, phases_list, title
):
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(phases_list)
        .reset_index()
    )

    results_std = results.groupby(f"params.{group_by_param}").std().fillna(0)
    x = results_avg[f"params.{group_by_param}"]

    for phase in phases_list:
        plt.errorbar(
            x,
            results_avg[f"metrics.makespan.{phase}"],
            results_std[f"metrics.makespan.{phase}"],
            label=f"{phase} time",
        )

    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel("Time [s]")
    plt.title(title)
    plt.savefig(plot_filename)


def plot_execution_time_and_request_time_ratio_vs_param(
    results, group_by_param, plot_filename, worker, title
):
    results["mean_execution_time"] = results.apply(
        lambda row: sum(row[f"metrics.workers_execution_times.{worker}"])
        / len(row[f"metrics.workers_execution_times.{worker}"])
        if len(row[f"metrics.workers_execution_times.{worker}"]) > 0
        else 0,
        axis=1,
    )
    
    results["mean_request_time"] = results.apply(
        lambda row: sum(row[f"metrics.workers_request_times.{worker}"])
        / len(row[f"metrics.workers_request_times.{worker}"])
        if len(row[f"metrics.workers_request_times.{worker}"]) > 0
        else 0,
        axis=1,
    )
    results["execution_to_request"] = results.apply(
        lambda row: row["mean_execution_time"] / row["mean_request_time"] * 100
        if row["mean_request_time"] > 0
        else 0,
        axis=1,
    )
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["execution_to_request"])
        .reset_index()
    )
    plt.errorbar(
        results_avg[f"params.{group_by_param}"], results_avg["execution_to_request"]
    )
    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"[%]")
    plt.ylim([0, 100])
    plt.title(title)
    plt.savefig(plot_filename)


def plot_cumulative_time_vs_params(
    results, group_by_param, plot_filename, worker, title
):
    results["execution_time_sum"] = results.apply(
        lambda row: sum(row[f"metrics.workers_execution_times.{worker}"]), axis=1
    )
    results["request_time_sum"] = results.apply(
        lambda row: sum(row[f"metrics.workers_request_times.{worker}"]), axis=1
    )
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["execution_time_sum", "request_time_sum"])
        .reset_index()
    )
    results_std = results.groupby(f"params.{group_by_param}").std().fillna(0)

    x = results_avg[f"params.{group_by_param}"]
    plt.errorbar(
        x,
        results_avg[f"execution_time_sum"],
        results_std[f"execution_time_sum"],
        label=f"{worker} executions cumulative time",
    )

    plt.errorbar(
        x,
        results_avg[f"execution_time_sum"],
        results_std[f"execution_time_sum"],
        label=f"{worker} requests cumulative time",
    )

    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel("Time [s]")
    plt.title(title)
    plt.savefig(plot_filename)


def plot_speedup(input_results_dump, group_by_param, plot_filename, title):
    ONE_PROCESS_TIME = 2000
    input_results_dump["speedup"] = input_results_dump.apply(
        lambda row: ONE_PROCESS_TIME / (row[f"metrics.total_duration"] - 4), axis=1
    )
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
