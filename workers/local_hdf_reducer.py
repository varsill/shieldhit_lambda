from common import load_hdf_result_file, separate_results
import glob
import numpy as np
from converters import Converters
import os
import h5py


def launch_worker(input_files_dir, output_dir):
  separate_results(input_files_dir, input_files_dir)
  results = {}
  for subdir in glob.glob(f"{input_files_dir}/*"):
    all_hdf_files = [hdf_file for hdf_file in glob.glob(f"{subdir}/*.h5")]
    cumulative = load_hdf_result_file(all_hdf_files[0])

    for file in all_hdf_files[1:]:
      cumulative=cumulative+load_hdf_result_file(hdf_file)

    _directory_path, just_file_name = os.path.split(subdir)
    output_file_name = f"{just_file_name}.h5"
    results[output_file_name] = cumulative/len(all_hdf_files) 
  return results