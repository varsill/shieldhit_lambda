from common import load_hdf_result_file, separate_results
import glob
import numpy as np
from converters import Converters
import os
import h5py
from datatypes.in_memory import InMemoryHDF

def launch_worker(input_files, output_file_name):
  all_hdf_files = input_files.to_memory().read_all()
  filenames = list(all_hdf_files.keys())
  head, *tail = filenames
  cumulative = all_hdf_files[head]

  for filename in tail:
    cumulative=cumulative+all_hdf_files[filename]

  return InMemoryHDF({output_file_name: cumulative/len(filenames)})

