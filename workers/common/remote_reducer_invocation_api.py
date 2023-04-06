import requests
from common import meassure_time
import os
from datatypes.in_memory import InMemoryBinary
import lzma
from typing import NewType
from workers.aws_bdo_reducer import launch_worker as launch_aws_bdo_reducer
from workers.whisk_bdo_reducer import launch_worker as launch_whisk_bdo_reducer
from datatypes.filesystem import FilesystemBinary, FilesystemHDF
import functools
from workers.common.remote import RemoteEnvironment


def send_request_to_remote_reducer(files, operation, lambda_url, worker_id_prefix, get_from):
  json_input = {"action": "reduce", "files": files.read_all(), "operation": operation, "N": worker_id_prefix, "get_from": get_from}
  response, request_time = meassure_time( lambda: requests.post(lambda_url, json=json_input, verify=False))

  if response.status_code != 200:
      print("send_request_to_remote_reducer ERROR", response.content)
      raise Exception(
          f"Remote reducer reponse unsuccessful! Reason: {response.content}"
      )
  
  result = {
      "status": "OK",
      "simulation_time": response.json()["time"],
      "request_time": request_time,
      "files": InMemoryBinary(response.json()["files"], transform=lzma.decompress)
  }
  return result




def resolve_remote_reducer(faas_environment: RemoteEnvironment):
    if faas_environment == "whisk":
       return launch_whisk_bdo_reducer
    elif faas_environment=="aws": 
        return launch_aws_bdo_reducer
    else:
        raise Exception(f"Unknown FaaS environment: {faas_environment}")