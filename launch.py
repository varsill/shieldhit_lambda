import os
import requests
from converters import *
import glob

lambda_url = os.getenv("LAMBDA_URL")
dat_files = files_to_map(glob.glob("input/*.dat"))
json_input = {"n": 1000, "N": 0, "files": dat_files}
result = requests.post(lambda_url, json=json_input)
map_to_files(result.json(), "./results")