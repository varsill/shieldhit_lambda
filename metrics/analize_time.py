import functools

import matplotlib.pyplot as plt


def _agregate_times(column, metric):
    if column.name != f"metrics.{metric}":
        return None
    else:
        result = []
        for row in column:
            result = result + row
        return result


def plot_request_times_histogram(
    results, group_by_param, metric, group_by_param_value
):
    results = results[results[f"params.{group_by_param}"] == group_by_param_value]
    agregator = functools.partial(_agregate_times, metric=metric)
    results_avg = (
        results.groupby(f"params.{group_by_param}").agg(agregator).reset_index()
    )
    
    all_times = results_avg[f"metrics.{metric}"].tolist()[0]
    plt.hist(all_times)
    plt.xlabel("Request time [s]")
    plt.ylabel(f"Number of occurences")


def plot_execution_time_to_request_time_vs_param(
    results, group_by_param, worker
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
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["mean_execution_time", "mean_request_time"])
        .reset_index()
    )
    plt.bar(
        results_avg[f"params.{group_by_param}"],
        results_avg["mean_request_time"],
        label="Request time",
        width=5
    )
    plt.bar(
        results_avg[f"params.{group_by_param}"]+5,
        results_avg["mean_execution_time"],
        label="Execution time",
        width=5
    )
    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"Time [s]")


def plot_percentage_of_successfull_responses(
    results, group_by_param, worker, label
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
        results_avg["successfull_responses_perc"]*100,
        marker=".",
        linestyle="None",
        label=label
    )
    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"% of successfull responses")
    plt.ylim([0, 1.1])

def plot_phases_makespan_vs_params(
    results, group_by_param, phases_list, labels
):
    results_avg = (
        results.groupby(f"params.{group_by_param}").mean(phases_list).reset_index()
    )
    results_avg += 3
    results_std = results.groupby(f"params.{group_by_param}").std().fillna(0)
    x = results_avg[f"params.{group_by_param}"]


    for phase, label in zip(phases_list, labels):
        plt.errorbar(
            x,
            results_avg[f"metrics.makespan.{phase}"],
            results_std[f"metrics.makespan.{phase}"],
            label=label,
            marker=".",
            linestyle="None",
        )

def plot_execution_time_and_request_time_ratio_vs_param(
    results, group_by_param, worker, label
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
        results_avg[f"params.{group_by_param}"], results_avg["execution_to_request"],
        marker=".",
        linestyle="None",
        label=label
    )
    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel(f"[%]")
    plt.ylim([0, 100])

def sum_all_workers_times(row, filter_col):
    cumulative_time = 0
    for worker in row[filter_col]:
        cumulative_time+=sum(worker)
    return cumulative_time



def plot_all_cumulative_time(results, group_by_param, label):
    filter_col = [col for col in results if col.startswith('metrics.workers_execution_times')]
    results["execution_time_sum"] = results.apply(
        lambda row: sum_all_workers_times(row,filter_col), axis=1
    )
    results_avg = (
        results.groupby(f"params.{group_by_param}")
        .mean(["execution_time_sum"])
        .reset_index()
    )
    results_std = results.groupby(f"params.{group_by_param}").std().fillna(0)

    x = results_avg[f"params.{group_by_param}"]
    plt.errorbar(
        x,
        results_avg[f"execution_time_sum"]+1200,
        results_std[f"execution_time_sum"],
        label=label,
        marker=".",
        linestyle="None",
    )


def plot_cumulative_time_vs_params(
    results, group_by_param, worker, color
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
        marker=".",
        linestyle="None",
        color=color
    )

    plt.errorbar(
        x,
        results_avg[f"request_time_sum"],
        results_std[f"request_time_sum"],
        label=f"{worker} requests cumulative time",
        marker=".",
        linestyle="None",
        color=color
    )
    
    plt.legend()
    plt.xlabel(f"{group_by_param}")
    plt.ylabel("Time [s]")

    xlab = plt.xaxis.get_label()
    ylab = plt.yaxis.get_label()

    xlab.set_style('italic')
    xlab.set_size(10)
    ylab.set_style('italic')
    ylab.set_size(10)

def plot_speedup(
    input_results_dump,
    group_by_param,
    phase,
    phase_with_single_worker_duration,
    non_paralelizable_phase_duration=1,
    color='g',
    label=''
):
    input_results_dump["speedup"] = input_results_dump.apply(
        lambda row: phase_with_single_worker_duration
        / (row[f"metrics.makespan.{phase}"]),
        axis=1,
    )
    results_avg = (
        input_results_dump.groupby(f"params.{group_by_param}")
        .mean(["speedup"])
        .reset_index()
    )
    k = (phase_with_single_worker_duration-non_paralelizable_phase_duration)/phase_with_single_worker_duration
    x = results_avg[f"params.{group_by_param}"]
    plt.errorbar(
        x,
        results_avg[f"speedup"],
        label=f"{label} - Speedup",
        marker=".",
        linestyle="None",
        c=color
    )

    plt.errorbar(
        x,
        1/(1-k+k/x),
        label=f"{label} - Maximum speedup according to Amdahl's law",
        linestyle="--",
        c=color
    )
