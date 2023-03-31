import argparse
from analize_time import *
from analize_distribution import *
import os
import pickle

def load(input_file_path):
    with open(input_file_path, "rb") as input_file:
        results = pickle.load(input_file)
    return results

parser = argparse.ArgumentParser(description='Analyze .dump files.')
parser.add_argument("input")
parser.add_argument("-g", "--group_by_param", default="number_of_workers")
parser.add_argument("-o", "--output", default="./")

subparsers = parser.add_subparsers(
    title="operation",
    dest="operation"
)

histogram_parser = subparsers.add_parser("histogram")
histogram_parser.add_argument("-m", "--metric", default="mappers_simulation_times")
histogram_parser.add_argument("--group_by_param_value", default=100)

success_ratio_parser = subparsers.add_parser("success_ratio")

simulation_to_request_parser = subparsers.add_parser("simulation_to_request")
simulation_to_request_parser.add_argument("-w", "--workers", default="mappers")

simulation_and_request_parser = subparsers.add_parser("simulation_and_request")
simulation_and_request_parser.add_argument("-w", "--workers", default="mappers")

times_parser = subparsers.add_parser("times")

distribution_parser = subparsers.add_parser("distribution")


args = parser.parse_args()
operation = args.operation
input_path = args.input
input_results_dump = load(input_path)
filename = input_path.split("/")[-1]
filename_without_extension, _extension = os.path.splitext(filename)
plot_filename = f"{args.output}/{filename_without_extension}_{operation}.png"

if operation == "histogram":
  plot_request_times_histogram(input_results_dump, args.group_by_param, plot_filename, metric=args.metric, group_by_param_value=args.group_by_param_value)

elif operation == "simulation_to_request":
  plot_simulation_time_to_request_time_vs_param(input_results_dump, args.group_by_param, plot_filename, workers=args.workers)

elif operation == "success_ratio":
  plot_percentage_of_successfull_responses(input_results_dump, args.group_by_param, plot_filename)

elif operation == "simulation_and_request":
  plot_simulation_time_and_request_time_ratio_vs_param(input_results_dump, args.group_by_param, plot_filename, workers=args.workers)

elif operation == "times":
  plot_map_reduce_total_time_vs_params(input_results_dump, args.group_by_param, plot_filename)

elif operation == "distribution":
  plot_distribution(input_results_dump, args.group_by_param, plot_filename)