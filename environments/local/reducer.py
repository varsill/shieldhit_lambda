from common import cmd
import shutil
import glob
import os
from environments.reducer import Reducer as ReducerInterface


class Reducer(ReducerInterface):
    """Module used for samples aggregation within local environment. Implements 'environment.reducer.Reducer' interface."""

    def __init__(self, input_files_dir: str, output_dir: str, operation: str = "image"):
        self.input_files_dir = input_files_dir
        self.output_dir = output_dir
        self.operation = operation

    def execute(self):
        self._separate_results(self.input_files_dir, self.input_files_dir)
        for subdir in glob.glob(f"{self.input_files_dir}/*"):
            cmd(
                f"binaries/convertmc.exe {self.operation} --many {subdir}/* {self.output_dir}"
            )

    def _separate_results(self, input_dir, output_dir):
        for filename in glob.glob(f"{input_dir}/*"):
            _directory_path, just_file_name = os.path.split(filename)
            result_name, _, _worker_id = just_file_name.rpartition("_")
            result_subdir = f"{output_dir}/{result_name}"
            os.makedirs(result_subdir, exist_ok=True)
            if os.path.exists(os.path.join(result_subdir, filename)):
              os.remove(os.path.join(result_subdir, filename))
            shutil.move(filename, result_subdir)
