import requests
from common import meassure_time, execute_concurrently
import os
from datatypes.in_memory import InMemoryBinary
import lzma
from typing import NewType
from workers.aws_mapper import launch_worker as launch_aws_mapper
from workers.whisk_mapper import launch_worker as launch_whisk_mapper
from datatypes.filesystem import FilesystemBinary, FilesystemHDF
from datatypes.in_memory import InMemoryBinary, InMemoryHDF
import functools
import glob

RemoteMapperEnvironment = NewType("RemoteMapperEnvironment", str)

def send_request_to_remote_mapper(worker_id, how_many_samples, files, lambda_url, should_produce_hdf):
  if should_produce_hdf:
    action = "map_and_reduce"
  else:
    action = "map"
  json_input = {"action": action, "n": how_many_samples, "N": worker_id, "files": files.read_all()}
  response, request_time = meassure_time( lambda: requests.post(lambda_url, json=json_input, verify=False))

  if response.status_code != 200:
      raise Exception(
          f"Worker {worker_id}: reponse unsuccessful! Reason: {response.content}"
      )
  
  result = {
      "status": "OK",
      "worker_id": worker_id,
      "simulation_time": response.json()["time"],
      "request_time": request_time,
      "files": InMemoryBinary(response.json()["files"], lzma.decompress)
  }
  return result


def launch_multiple_remote_mappers(how_many_samples: int, how_many_workers: int, dat_files: InMemoryBinary, should_mapper_produce_hdf: bool, launch_mapper):  
  samples_per_worker = int(how_many_samples / how_many_workers)
  mapper_function = functools.partial(
      launch_mapper,
      how_many_samples=samples_per_worker,
      files=dat_files,
      should_produce_hdf=should_mapper_produce_hdf
  )
  
  mapper_results, map_time = meassure_time(lambda: execute_concurrently(mapper_function, how_many_workers))
  successfull_mapper_results = [r for r in mapper_results if r["status"]=="OK"]
  print(f"SUCCESS/ALL: {len(successfull_mapper_results)}/{how_many_workers}")
  
  workers_times = []

  in_memory_data_to_return = InMemoryBinary({}, lzma.decompress)
  for r in successfull_mapper_results:
      in_memory_data_to_return.merge(r["files"])
      del r["files"]
      workers_times.append(r)
  return in_memory_data_to_return, map_time, workers_times


def resolve_remote_mapper(faas_environment: RemoteMapperEnvironment):
    if faas_environment == "whisk":
       return launch_whisk_mapper
    elif faas_environment=="aws": 
        return launch_aws_mapper
    else:
        raise Exception(f"Unknown FaaS environment: {faas_environment}")