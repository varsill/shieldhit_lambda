import requests
from common import meassure_time
from datatypes.in_memory import InMemoryBinary
import lzma
from workers.aws.extract_and_reduce import extract_and_reduce as aws_extract_and_reduce
from workers.aws.simulate import simulate as aws_simulate
from workers.aws.simulate_and_extract import simulate_and_extract as aws_simulate_and_extract

from workers.whisk.extract_and_reduce import extract_and_reduce as whisk_extract_and_reduce
from workers.whisk.simulate import simulate as whisk_simulate
from workers.whisk.simulate_and_extract import simulate_and_extract as whisk_simulate_and_extract

from datatypes.in_memory import InMemoryBinary
from workers.common.remote import RemoteEnvironment


def send_request_to_remote_simulate(worker_id, how_many_samples, files, lambda_url, save_to):
  json_input = {"action": "simulate", "n": how_many_samples, "N": worker_id, "files": files.read_all(), "save_to": save_to}
  response, request_time = meassure_time( lambda: requests.post(lambda_url, json=json_input, verify=False))
  if response.status_code != 200:
      raise Exception(
          f"Worker {worker_id}: reponse unsuccessful! Reason: {response.content}"
      )
  
  result = {
      "status": "OK",
      "worker_id": worker_id,
      "execution_time": response.json()["time"],
      "request_time": request_time,
      "files": InMemoryBinary(response.json()["files"], lzma.decompress)
  }
  return result


def send_request_to_remote_simulate_and_extract(worker_id, how_many_samples, files, lambda_url, save_to):
  json_input = {"action": "simulate_and_extract", "n": how_many_samples, "N": worker_id, "files": files.read_all(), "save_to": save_to}
  response, request_time = meassure_time( lambda: requests.post(lambda_url, json=json_input, verify=False))
  if response.status_code != 200:
      raise Exception(
          f"Worker {worker_id}: reponse unsuccessful! Reason: {response.content}"
      )
  
  result = {
      "status": "OK",
      "worker_id": worker_id,
      "execution_time": response.json()["time"],
      "request_time": request_time,
      "files": InMemoryBinary(response.json()["files"], lzma.decompress)
  }
  return result

def send_request_to_remote_extract_and_reduce(files, lambda_url, worker_id_prefix, get_from):
  json_input = {"action": "extract_and_reduce", "files": files.read_all(), "N": worker_id_prefix, "get_from": get_from}
  response, request_time = meassure_time(lambda: requests.post(lambda_url, json=json_input, verify=False))

  if response.status_code != 200:
      print("send_request_to_remote_reducer ERROR", response.content)
      raise Exception(
          f"Remote reducer reponse unsuccessful! Reason: {response.content}"
      )
  
  result = {
      "status": "OK",
      "execution_time": response.json()["time"],
      "request_time": request_time,
      "files": InMemoryBinary(response.json()["files"], transform=lzma.decompress)
  }
  return result

def resolve_remote_function(action, faas_environment: RemoteEnvironment):
    # TODO - refactor with importlib
    if faas_environment == "whisk":
      if action == "simulate":
        return whisk_simulate
      elif action == "simulate_and_extract":
        return whisk_simulate_and_extract
      elif action == "extract_and_reduce":
        return whisk_extract_and_reduce
      else:
         raise Exception(f"Unknown action: {action}")
    elif faas_environment == "aws":
      if action == "simulate":
        return aws_simulate
      elif action == "simulate_and_extract":
        return aws_simulate_and_extract
      elif action == "extract_and_reduce":
        return aws_extract_and_reduce
      else:
         raise Exception(f"Unknown action: {action}")
    else:
        raise Exception(f"Unknown FaaS environment: {faas_environment}")