import base64
import json
import os

def __read_dat_file(file_name):
  with open(file_name, "rb") as file:
    content = file.read()
    return str(base64.b64encode(content))[2:-1]

def files_to_map(input_filenames):
  result = {}
  for filename in input_filenames:
    encoded = __read_dat_file(filename)
    _directory_path, just_file_name = os.path.split(filename)
    result[just_file_name] = encoded
  return result
  
def files_to_json(input_filenames, output_path):
  result_map = files_to_map(input_filenames)
  with open(output_path, "w") as output_file:
    output_file.write(json.dumps(result_map))


def map_to_files(input_map, output_directory):
  for result_file_name in input_map.keys():
    with open(f"{output_directory}/{result_file_name}", "wb") as result_file:
      decoded = base64.b64decode(input_map[result_file_name])
      result_file.write(decoded)

def json_to_files(input_path, output_directory):
  with open(input_path, "r") as input:
    content = str(input.read())
    input_map = json.loads(content)
    map_to_files(input_map, output_directory)