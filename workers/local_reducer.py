from common import cmd
import shutil
import glob
import os

def launch_worker(input_files_dir: str, output_dir: str, operation: str = "image"):
    __separate_results(input_files_dir, input_files_dir)
    for subdir in glob.glob(f"{input_files_dir}/*"):
        cmd(
            f"binaries/convertmc {operation} --many {subdir}/* {output_dir}"
        )

def __separate_results(input_dir, output_dir):
    for filename in glob.glob(f"{input_dir}/*"):
        _directory_path, just_file_name = os.path.split(filename)
        result_name, _, _worker_id = just_file_name.rpartition("_")
        result_subdir = f"{output_dir}/{result_name}"
        os.makedirs(result_subdir, exist_ok=True)
        if os.path.exists(os.path.join(result_subdir, filename)):
          os.remove(os.path.join(result_subdir, filename))
        shutil.move(filename, result_subdir)
