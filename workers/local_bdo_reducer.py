from common import cmd, separate_results
import shutil
import glob
import os

def launch_worker(input_files_dir: str, output_dir: str, operation: str = "image"):
    separate_results(input_files_dir, input_files_dir)
    for subdir in glob.glob(f"{input_files_dir}/*"):
        cmd(
            f"binaries/convertmc {operation} --many {subdir}/* {output_dir}"
        )

