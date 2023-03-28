from dataclasses import dataclass
from converters import Converters, id
import glob
from common import load_hdf_result_file
from typing import Any
import os


@dataclass
class FilesystemBinary:
    directory: str
    files_pattern: str = "*"
    transform: Any = id

    def to_memory(self):
        from datatypes.in_memory import InMemoryBinary

        all_files = glob.glob(f"{self.directory}/{self.files_pattern}")
        return InMemoryBinary(Converters.files_to_map(all_files, self.transform))

    def get_directory(self):
        return self.directory

    def to_hdf(self):
        if self.transform != id:
            raise "Cannot perform transition from FilesystemBinary to FilesystemHDF since the transform is not id!"
        return FilesystemHDF(self.directory, self.files_pattern)


@dataclass
class FilesystemHDF:
    directory: str
    files_pattern: str = "*"

    def to_memory(self):
        from datatypes.in_memory import InMemoryHDF

        all_files = glob.glob(f"{self.directory}/{self.files_pattern}")
        results = {}
        for file in all_files:
            _directory_path, just_file_name = os.path.split(file)
            results[just_file_name] = load_hdf_result_file(file)
        return InMemoryHDF(results)

    def get_directory(self):
        return self.directory

    def to_binary(self):
        return FilesystemBinary(self.directory, self.files_pattern, id)
