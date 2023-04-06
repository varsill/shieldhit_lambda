from dataclasses import dataclass
from typing import Any, Dict

import h5py

from common import mktemp
from converters import Converters
from datatypes.filesystem import FilesystemBinary, FilesystemHDF


@dataclass
class InMemoryBinary:
    files_map: Dict
    transform: Any = None

    def to_filesystem(self, output_dir, use_memfd=False):

        Converters.map_to_files(self.files_map, output_dir, self.transform, use_memfd)
        return FilesystemBinary(output_dir)

    def read(self, filename):
        return self.files_map[filename]

    def read_all(self):
        return self.files_map

    def merge(self, other):
        if isinstance(other, dict):
            self.files_map = {**self.files_map, **other}
        else:
            self.files_map = {**self.files_map, **other.files_map}

    def to_hdf(self):
        tmdir = mktemp()
        return self.to_filesystem(tmdir, use_memfd=False).to_hdf().to_memory()


@dataclass
class InMemoryHDF:
    files_map: Dict

    def to_filesystem(self, output_dir):

        for filename in self.files_map.keys():
            with h5py.File(f"{output_dir}/{filename}", "w") as f:
                if isinstance(self.files_map[filename], list):
                    for i in range(len(self.files_map[filename])):
                        f.create_dataset(f"data_{i}", data=self.files_map[filename][i])
                else:
                    f.create_dataset("data", data=self.files_map[filename])

        return FilesystemHDF(output_dir)

    def read(self, filename):
        return self.files_map[filename]

    def read_all(self):
        return self.files_map

    def merge(self, other):
        if isinstance(other, dict):
            self.files_map = {**self.files_map, **other}
        else:
            self.files_map = {**self.files_map, **other.files_map}
