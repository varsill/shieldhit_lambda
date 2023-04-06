#!/usr/bin/python3

import argparse
import os
import pickle
from textwrap import wrap

from analize_distribution import *
from analize_time import *


def load(input_file_path):
    with open(input_file_path, "rb") as input_file:
        results = pickle.load(input_file)
    return results


parser = argparse.ArgumentParser(description="Analyze .dump files.")
parser.add_argument("input")
parser.add_argument("-g", "--group_by_param", default="number_of_workers")
parser.add_argument("-o", "--output", default="./")
parser.add_argument("-l", "--limit", default="0,500")
parser.add_argument("-t", "--title", default="")

subparsers = parser.add_subparsers(title="operation", dest="operation")

histogram_parser = subparsers.add_parser("histogram")
histogram_parser.add_argument("-m", "--metric", default="mappers_simulation_times")
histogram_parser.add_argument("--group_by_param_value", default=100)

success_ratio_parser = subparsers.add_parser("success_ratio")

simulation_to_request_parser = subparsers.add_parser("simulation_to_request")
simulation_to_request_parser.add_argument("-w", "--workers", default="mappers")

simulation_request_ratio_parser = subparsers.add_parser("simulation_request_ratio")
simulation_request_ratio_parser.add_argument("-w", "--workers", default="mappers")

times_parser = subparsers.add_parser("times")
times_parser.add_argument(
    "-f", "--fields", default="map_time,reduce_time,total_duration"
)

distribution_parser = subparsers.add_parser("distribution")

cumulative_times_parser = subparsers.add_parser("cumulative_times")
cumulative_times_parser.add_argument("-w", "--workers", default="mappers")

speedup_parser = subparsers.add_parser("speedup")


args = parser.parse_args()
operation = args.operation
input_path = args.input
input_results_dump = load(input_path)
filename = input_path.split("/")[-1]
filename_without_extension, _extension = os.path.splitext(filename)
plot_filename = f"{args.output}/{filename_without_extension}_{operation}.png"

input_results_dump = pd.json_normalize(input_results_dump)
limit = args.limit.split(",")
lower_limit = int(limit[0])
upper_limit = int(limit[1])
input_results_dump = input_results_dump[
    input_results_dump[f"params.{args.group_by_param}"] > lower_limit
]
input_results_dump = input_results_dump[
    input_results_dump[f"params.{args.group_by_param}"] < upper_limit
]
args.title = "\n".join(wrap(args.title))
if operation == "histogram":
    plot_request_times_histogram(
        input_results_dump,
        args.group_by_param,
        plot_filename,
        metric=args.metric,
        group_by_param_value=args.group_by_param_value,
        title=args.title,
    )

elif operation == "simulation_to_request":
    plot_simulation_time_to_request_time_vs_param(
        input_results_dump,
        args.group_by_param,
        plot_filename,
        workers=args.workers,
        title=args.title,
    )

elif operation == "success_ratio":
    plot_percentage_of_successfull_responses(
        input_results_dump, args.group_by_param, plot_filename, title=args.title
    )

elif operation == "simulation_request_ratio":
    plot_simulation_time_and_request_time_ratio_vs_param(
        input_results_dump,
        args.group_by_param,
        plot_filename,
        workers=args.workers,
        title=args.title,
    )

elif operation == "times":
    fields_list = args.fields.split(",")
    plot_map_reduce_total_time_vs_params(
        input_results_dump,
        args.group_by_param,
        plot_filename,
        fields_list,
        title=args.title,
    )

elif operation == "distribution":
    plot_distribution(
        input_results_dump, args.group_by_param, plot_filename, title=args.title
    )

elif operation == "cumulative_times":
    plot_cumulative_time_vs_params(
        input_results_dump,
        args.group_by_param,
        plot_filename,
        workers=args.workers,
        title=args.title,
    )

elif operation == "speedup":
    plot_speedup(
        input_results_dump, args.group_by_param, plot_filename, title=args.title
    )
