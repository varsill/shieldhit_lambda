from common import load_hdf_result_file, separate_results, mktemp, meassure_time
import glob
import numpy as np
from converters import Converters
import os
import h5py
from datatypes.in_memory import InMemoryHDF
from datatypes.filesystem import FilesystemHDF


def launch_worker(input_files: FilesystemHDF):
    tmpdir = mktemp()
    separate_results(input_files.get_directory(), tmpdir)

    cumulative_result = InMemoryHDF({})
    cumulative_reduce_time = 0

    for subdir in glob.glob(f"{tmpdir}/*"):
        _directory_path, just_file_name = os.path.split(subdir)
        output_file_name = f"{just_file_name}.h5"
        reducer_result, reduce_time = meassure_time(
            lambda: __calculate_mean(
                FilesystemHDF(subdir).to_memory().read_all(), output_file_name
            )
        )
        cumulative_result.merge(reducer_result)
        cumulative_reduce_time += reduce_time

    return cumulative_result, cumulative_reduce_time


def __calculate_mean(hdf_files_map, output_file_name):
    filenames = list(hdf_files_map.keys())
    head, *tail = filenames
    cumulative = hdf_files_map[head]

    for filename in tail:
        cumulative = cumulative + hdf_files_map[filename]

    return {output_file_name: cumulative / len(filenames)}
