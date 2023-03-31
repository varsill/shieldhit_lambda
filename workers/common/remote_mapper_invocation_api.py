import requests
from common import meassure_time, execute_concurrently
import os
from datatypes.in_memory import InMemoryBinary
import lzma
from workers.aws_mapper import launch_worker as launch_aws_mapper
from workers.whisk_mapper import launch_worker as launch_whisk_mapper
from datatypes.filesystem import FilesystemBinary, FilesystemHDF
from datatypes.in_memory import InMemoryBinary, InMemoryHDF
import functools
import glob
from workers.common.remote import RemoteEnvironment


def send_request_to_remote_mapper(worker_id, how_many_samples, files, lambda_url, should_produce_hdf, save_to):
  if should_produce_hdf:
    action = "map_and_reduce"
  else:
    action = "map"
  json_input = {"action": action, "n": how_many_samples, "N": worker_id, "files": files.read_all(), "save_to": save_to}
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

def resolve_remote_mapper(faas_environment: RemoteEnvironment):
    if faas_environment == "whisk":
       return launch_whisk_mapper
    elif faas_environment=="aws": 
        return launch_aws_mapper
    else:
        raise Exception(f"Unknown FaaS environment: {faas_environment}")