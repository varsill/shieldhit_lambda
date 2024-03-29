#!/usr/bin/python3

import argparse
import os
import pickle
from textwrap import wrap

from analize_distribution import *
from analize_time import *
import matplotlib.pyplot as plt


def load(input_file_path):
    with open(input_file_path, "rb") as input_file:
        results = pickle.load(input_file)
    return results

def prepare_input(input_path, lower_limit, upper_limit, group_by_param):
    input_results_dump = load(input_path)
    input_results_dump = pd.json_normalize(input_results_dump)
    input_results_dump = input_results_dump[
        input_results_dump[f"params.{group_by_param}"] > lower_limit
    ]
    input_results_dump = input_results_dump[
        input_results_dump[f"params.{group_by_param}"] < upper_limit
    ]
    return input_results_dump

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze .dump files.")
    parser.add_argument("input")
    parser.add_argument("-g", "--group_by_param", default="how_many_workers")
    parser.add_argument("-o", "--output", default="./")
    parser.add_argument("-l", "--limit", default="0,500")
    parser.add_argument("-t", "--title", default="")

    subparsers = parser.add_subparsers(title="operation", dest="operation")

    histogram_parser = subparsers.add_parser("histogram")
    histogram_parser.add_argument(
        "-m", "--metric", default="workers_request_times.simulate"
    )
    histogram_parser.add_argument("--group_by_param_value", default=100)

    success_ratio_parser = subparsers.add_parser("success_ratio")
    success_ratio_parser.add_argument("-w", "--worker", default="simulate")

    execution_and_request_parser = subparsers.add_parser("execution_and_request")
    execution_and_request_parser.add_argument("-w", "--worker", default="simulate")

    execution_to_request_ratio_parser = subparsers.add_parser("execution_to_request_ratio")
    execution_to_request_ratio_parser.add_argument("-w", "--worker", default="simulate")

    phases_makespan_parser = subparsers.add_parser("phases_makespan")
    phases_makespan_parser.add_argument("-p", "--phases", default="total")

    distribution_parser = subparsers.add_parser("distribution")

    mse_parser = subparsers.add_parser("mse")

    psnr_parser = subparsers.add_parser("psnr")

    cumulative_times_parser = subparsers.add_parser("cumulative_workers_times")
    cumulative_times_parser.add_argument("-w", "--worker", default="simulate")

    speedup_parser = subparsers.add_parser("speedup")
    speedup_parser.add_argument("-p", "--phase", default="simulating")
    speedup_parser.add_argument("--phase_with_single_worker_duration", default=1)

    args = parser.parse_args()
    operation = args.operation
    input_path = args.input
    limit = args.limit.split(",")
    lower_limit = int(limit[0])
    upper_limit = int(limit[1])
    args.title = "\n".join(wrap(args.title))

    input_results_dump = prepare_input(input_path, lower_limit, upper_limit, args.group_by_param)

    filename = input_path.split("/")[-1]
    filename_without_extension, _extension = os.path.splitext(filename)
    plot_filename = f"{args.output}/{filename_without_extension}_{operation}.png"


    print(f"SAVING TO: {plot_filename}")
    if operation == "histogram":
        plot_request_times_histogram(
            input_results_dump,
            args.group_by_param,
            metric=args.metric,
            group_by_param_value=int(args.group_by_param_value),
        )

    elif operation == "execution_and_request":
        plot_execution_time_to_request_time_vs_param(
            input_results_dump,
            args.group_by_param,
            worker=args.worker
        )

    elif operation == "success_ratio":
        plot_percentage_of_successfull_responses(
            input_results_dump,
            args.group_by_param,
            worker=args.worker
        )

    elif operation == "execution_to_request_ratio":
        plot_execution_time_and_request_time_ratio_vs_param(
            input_results_dump,
            args.group_by_param,
            worker=args.worker
        )

    elif operation == "phases_makespan":
        phases_list = args.phases.split(",")
        plot_phases_makespan_vs_params(
            input_results_dump,
            args.group_by_param,
            phases_list,
        )

    elif operation == "distribution":
        plot_distribution(
            input_results_dump, args.group_by_param
        )

    elif operation == "mse":
        plot_mse(input_results_dump, args.group_by_param)

    elif operation == "psnr":
        plot_psnr(input_results_dump, args.group_by_param)

    elif operation == "cumulative_workers_times":
        plot_cumulative_time_vs_params(
            input_results_dump,
            args.group_by_param,
            worker=args.worker,
        )

    elif operation == "speedup":
        plot_speedup(
            input_results_dump,
            args.group_by_param,
            args.phase,
            int(args.phase_with_single_worker_duration),
            title=args.title,
        )
    plt.title(args.title)
    plt.savefig(plot_filename)