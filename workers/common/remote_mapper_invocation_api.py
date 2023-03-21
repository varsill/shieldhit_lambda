import requests
from common import meassure_time, deserialize
import os
from datatypes.in_memory import InMemoryBinary
import lzma

os.environ["PYTHONWARNINGS"]="ignore:Unverified HTTPS request"

def invoke_remote_mapper(worker_id, how_many_samples, files, lambda_url, should_produce_hdf):
  json_input = {"n": how_many_samples, "N": worker_id, "files": files, "should_produce_hdf": should_produce_hdf}
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
      "files": response.json()["files"]
  }
  return result