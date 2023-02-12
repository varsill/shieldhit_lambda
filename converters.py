import base64
import json
import os

def __id(x):
  return x

def __read_file(file_name, transform):
  with open(file_name, "rb") as file:
    content = file.read()
    transformed = transform(content)
    encoded = base64.b64encode(transformed)
    encoded_str = encoded.decode("utf-8")
    return encoded_str

def files_to_map(input_filenames, transform=__id):
  result = {}
  for filename in input_filenames:
    encoded = __read_file(filename, transform)
    _directory_path, just_file_name = os.path.split(filename)
    result[just_file_name] = encoded
  return result
  
def files_to_json_file(input_filenames, output_path, transform=__id):
  result_map = files_to_map(input_filenames, transform)
  with open(output_path, "w") as output_file:
    output_file.write(json.dumps(result_map))


def map_to_files(input_map, output_directory, transform=__id):
  for result_file_name in input_map.keys():
    with open(f"{output_directory}/{result_file_name}", "wb") as result_file:
      content = input_map[result_file_name]
      decoded = base64.b64decode(content)
      transformed = transform(decoded)
      result_file.write(transformed)

def json_file_to_files(input_path, output_directory, transform=False):
  with open(input_path, "rb") as input:
    content = input.read().decode("utf-8")
    input_map = json.loads(content)
    map_to_files(input_map, output_directory, transform)