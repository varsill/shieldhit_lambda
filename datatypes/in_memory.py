from dataclasses import dataclass
from converters import Converters
import glob
from typing import Dict, Any

import h5py

@dataclass
class InMemoryBinary:
    files_map: Dict
    transform: Any = None

    def to_filesystem(self, output_dir):
        from datatypes.filesystem import FilesystemBinary

        Converters.map_to_files(self.files_map, output_dir, self.transform)
        return FilesystemBinary(output_dir)

    def read(self, filename):
        return self.files_map[filename]

    def read_all(self):
        return self.files_map


@dataclass
class InMemoryHDF:
    files_map: Dict

    def to_filesystem(self, output_dir):
        from datatypes.filesystem import FilesystemHDF

        for filename in self.files_map.keys():
            with h5py.File(f"{output_dir}/{filename}", "w") as f:
                f.create_dataset("data", data=self.files_map[filename])

        return FilesystemHDF(output_dir)

    def read(self, filename):
        return self.files_map[filename]

    def read_all(self):
        return self.files_map
