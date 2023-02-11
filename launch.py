import os
import shutil
import subprocess
import requests
from converters import *
import glob
from multiprocessing import Pool
import time
import functools

LAMBDA_URL = os.getenv("LAMBDA_URL")
DAT_FILES = files_to_map(glob.glob("input/*.dat"))
TEMPORARY_RESULTS = "results/temporary"
FINAL_RESULTS = "results/final"

def _cmd(command: str):
  return subprocess.check_output(command.split())

def meassure_time(f):
  start_time = time.time()
  result = f()
  end_time = time.time()
  return result, end_time-start_time

def _launch_worker(worker_id, how_many_samples):
  json_input = {"n": how_many_samples, "N": worker_id, "files": DAT_FILES}
  result, request_time = meassure_time(lambda: requests.post(LAMBDA_URL, json=json_input))
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

def execute_map(how_many_samples, how_many_workers):
  samples_per_worker = int(how_many_samples/how_many_workers)
  worker = functools.partial(_launch_worker, how_many_samples=samples_per_worker)
  with Pool(how_many_workers) as process:
    result = process.map_async(
      worker, 
      range(how_many_workers)
    )
    result.wait()
  return {"mappers_time": result.get()}

def execute_reduce(output_dir):
  _separate_results(TEMPORARY_RESULTS, TEMPORARY_RESULTS)
  for subdir in glob.glob(f"{TEMPORARY_RESULTS}/*"):
    _cmd(f"binaries/convertmc.exe txt --many {subdir}/* {output_dir}")
  shutil.rmtree(TEMPORARY_RESULTS)

def run(how_many_samples, how_many_workers):
  metrics, map_time = meassure_time(
    lambda: execute_map(how_many_samples, how_many_workers)
  )

  os.makedirs(FINAL_RESULTS, exist_ok=True)
  _, reduce_time = meassure_time(lambda: execute_reduce(FINAL_RESULTS))
  metrics["full_map_time"] = map_time
  metrics["reduce_time"] = reduce_time
  return metrics


    
  
    

  

