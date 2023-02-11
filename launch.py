import os
import shutil
import subprocess
import requests
from converters import *
import glob
from multiprocessing import Pool

HOW_MANY_WORKERS = 10
SAMPLES_PER_WORKER = 1000
LAMBDA_URL = os.getenv("LAMBDA_URL")
DAT_FILES = files_to_map(glob.glob("input/*.dat"))

def _cmd(command: str):
  return subprocess.check_output(command.split())

def _launch_worker(worker_id):
  json_input = {"n": SAMPLES_PER_WORKER, "N": worker_id, "files": DAT_FILES}
  result = requests.post(LAMBDA_URL, json=json_input)
  os.makedirs("results/temporary", exist_ok=True)
  map_to_files(result.json(), "results/temporary")

def _separate_results(input_dir, output_dir):
  for filename in glob.glob(f"{input_dir}/*"):
    directory_path, just_file_name = os.path.split(filename)
    result_name, _, worker_id = just_file_name.rpartition("_")
    result_subdir = f"{output_dir}/{result_name}"
    os.makedirs(result_subdir, exist_ok=True)
    shutil.move(filename, result_subdir)

def execute_map():
  with Pool(HOW_MANY_WORKERS) as process:
    result = process.map_async(_launch_worker, range(HOW_MANY_WORKERS))
    result.wait()
  
def execute_reduce(output_dir):
  _separate_results("results/temporary", "results/temporary")
  for subdir in glob.glob("results/temporary/*"):
    _cmd(f"binaries/convertmc.exe image --many {subdir}/* {output_dir}")
  shutil.rmtree("results/temporary")

if __name__ == '__main__':
  execute_map()
  os.makedirs("results/final", exist_ok=True)
  execute_reduce("results/final")
  
  
    

  

