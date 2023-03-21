from dataclasses import dataclass
from converters import Converters
import glob 
from common import load_hdf_result_file
from typing import Any
import os 

@dataclass
class FilesystemBinary:
  directory: str
  transform: Any = None
  
  def to_memory(self):
    from datatypes.in_memory import InMemoryBinary
    all_files = glob.glob(f"{self.directory}/*")
    return InMemoryBinary(Converters.files_to_map(all_files, self.transform))
  
  def get_directory(self):
    return self.directory


@dataclass
class FilesystemHDF:
  directory: str
  
  def to_memory(self):
    from datatypes.in_memory import InMemoryHDF
    all_files = glob.glob(f"{self.directory}/*")
    results = {}
    for file in all_files:
      _directory_path, just_file_name = os.path.split(file)
      results[just_file_name] = load_hdf_result_file(file)
    return InMemoryHDF(results)
  
  def get_directory(self):
    return self.directory