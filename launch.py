import os
import shutil
import subprocess
import requests
from converters import *
import glob
from multiprocessing import Pool
import time

HOW_MANY_WORKERS = 50

HOW_MANY_SAMPLES = 100000
SAMPLES_PER_WORKER = int(HOW_MANY_SAMPLES/HOW_MANY_WORKERS)
LAMBDA_URL = os.getenv("LAMBDA_URL")
DAT_FILES = files_to_map(glob.glob("input/*.dat"))
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"

def _cmd(command: str):
  return subprocess.check_output(command.split())

def _meassure_time(f):
  start_time = time.time()
  result = f()
  end_time = time.time()
  return result, end_time-start_time

def _launch_worker(worker_id):
  json_input = {"n": SAMPLES_PER_WORKER, "N": worker_id, "files": DAT_FILES}
  result, request_time = _meassure_time(lambda: requests.post(LAMBDA_URL, json=json_input))
  os.makedirs(TEMPORARY_RESULTS, exist_ok=True)
  map_to_files(result.json()["files"],TEMPORARY_RESULTS)
  metrics = {"simulation_time": result.json()["time"], "request_time": request_time}
  return metrics

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
  return {"mappers_time": result.get()}

def execute_reduce(output_dir):
  _separate_results(TEMPORARY_RESULTS, TEMPORARY_RESULTS)
  for subdir in glob.glob(f"{TEMPORARY_RESULTS}/*"):
    _cmd(f"binaries/convertmc.exe image --many {subdir}/* {output_dir}")
  shutil.rmtree(TEMPORARY_RESULTS)

def run():
  metrics, map_time = _meassure_time(execute_map)
  os.makedirs(FINAL_RESULTS, exist_ok=True)
  _, reduce_time = _meassure_time(lambda: execute_reduce(FINAL_RESULTS))
  metrics["full_map_time"] = map_time
  metrics["reduce_time"] = reduce_time
  return metrics

if __name__ == '__main__':
  metrics, duration = _meassure_time(run)
  metrics["total_duration"] = duration
  print(f"Metrics: {metrics}")
    
  
    

  

