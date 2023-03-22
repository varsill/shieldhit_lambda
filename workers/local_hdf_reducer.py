from common import load_hdf_result_file, separate_results, mktemp, meassure_time
import glob
import numpy as np
from converters import Converters
import os
import h5py
from datatypes.in_memory import InMemoryHDF
from datatypes.filesystem import FilesystemHDF


def launch_worker(input_files: FilesystemHDF, output_dir):
    tmpdir = mktemp()
    separate_results(input_files.get_directory(), tmpdir)

    cumulative_reduce_time = 0
    for subdir in glob.glob(f"{tmpdir}/*"):
        _directory_path, just_file_name = os.path.split(subdir)
        output_file_name = f"{just_file_name}.h5"
        reducer_result, reduce_time = meassure_time(
            lambda: __calculate_mean(FilesystemHDF(subdir), output_file_name)
        )
        cumulative_reduce_time += reduce_time
        if subdir.endswith("/z_profile"):
            hdf_sample = reducer_result.read("z_profile.h5")
        reducer_result.to_filesystem(output_dir)
    return FilesystemHDF(output_dir), cumulative_reduce_time, hdf_sample


def __calculate_mean(input_files, output_file_name):
    all_hdf_files = input_files.to_memory().read_all()
    filenames = list(all_hdf_files.keys())
    head, *tail = filenames
    cumulative = all_hdf_files[head]

    for filename in tail:
        cumulative = cumulative + all_hdf_files[filename]

    return InMemoryHDF({output_file_name: cumulative / len(filenames)})
