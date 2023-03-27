import subprocess
import glob
import os
from common import mktemp
from converters import Converters, id
import lzma
import boto3
import string
import random

BUCKET = "shieldhit-results-bucket"

def execute(event):
    action = event.get("action", "action not provided")
    if action=="map":
        n = event.get("n", 1000)
        N = event.get("N", 0)
        files = event["files"]
        save_to = event.get("save_to", "download")
        return mapper(n, N, files, False, save_to)
    elif action=="map_and_reduce":
        n = event.get("n", 1000)
        N = event.get("N", 0)
        files = event["files"]
        save_to = event.get("save_to", "download")
        return mapper(n, N, files, True, save_to)
    elif action=="reduce":
        files = event["files"]
        get_from = event.get("get_from", "uploaded")
        operation = event["operation"]
        worker_id_prefix = event["N"]
        return reducer(files, get_from, operation, worker_id_prefix)
    else:
        raise Exception(f"Unknown action: {action}")
    

def mapper(n, N, files, should_produce_hdf, save_to):
    try:
        subprocess.check_output(["chmod", "a+x", "shieldhit"])
        if should_produce_hdf:
            subprocess.check_output(["chmod", "a+x", "convertmc"])
    except Exception:
        pass
    
    tmpdir = mktemp()
   
    Converters.map_to_files(files, tmpdir, lzma.decompress)
     
    subprocess.check_output(["./shieldhit", "-n", str(n), "-N", str(N), tmpdir])

    if should_produce_hdf:
        subprocess.check_output(["./convertmc", "hdf", "--many", f"{tmpdir}/*.bdo", tmpdir])
        all_hdf_files = glob.glob(f"{tmpdir}/*.h5")
        all_result_files = _rename_hdf_files(all_hdf_files, str(N))
    else:
        all_result_files = glob.glob(f"{tmpdir}/*.bdo")

    if save_to == "s3":
        bucket_dir =  _get_random_string()
        client = boto3.client('s3')
        results_map = {}
        for filename in all_result_files:
            directory_path, just_file_name = os.path.split(filename)
            f = open(filename, "rb")
            client.put_object(Body=f.read(), Bucket=BUCKET, Key=f"{bucket_dir}/{just_file_name}")
            f.close()
            results_map[just_file_name] = f"{bucket_dir}/{just_file_name}"
        subprocess.check_output(["rm", "-rf", tmpdir])
        return results_map
    elif save_to == "download":
        result_map = Converters.files_to_map(all_result_files, transform=lzma.compress)
        subprocess.check_output(["rm", "-rf", tmpdir])
        return result_map

def reducer(files, get_from, operation, worker_id_prefix):
    try:
        subprocess.check_output(["chmod", "a+x", "convertmc"])
    except Exception:
        pass
    tmpdir = mktemp()
    if get_from == "uploaded":
        Converters.map_to_files(files, tmpdir, lzma.decompress)
    elif get_from == "s3":
        client = boto3.client('s3')
        for filename in files.keys():
            client.download_file(BUCKET, files[filename], f"{tmpdir}/{filename}")
    subprocess.check_output(["./convertmc", operation, "--many", f"{tmpdir}/*.bdo", tmpdir])

    #x = subprocess.run(f"cat {tmpdir}/{filename}", shell=True, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if operation == "image":
        extension = ".png"
    elif operation == "hdf":
        extension = ".h5"
    all_hdf_files = glob.glob(f"{tmpdir}/*{extension}")
    all_hdf_files_with_changed_name = _rename_hdf_files(all_hdf_files, worker_id_prefix)
    result_map = Converters.files_to_map(all_hdf_files_with_changed_name, lzma.compress)
    return result_map

def _rename_hdf_files(all_hdf_files, worker_id_prefix=""):
    all_hdf_files_with_changed_name = []
    for filename in all_hdf_files:
        just_filename, extension = os.path.splitext(filename)
        all_hdf_files_with_changed_name.append(f"{just_filename}{worker_id_prefix}{extension}")
    for old_filename, new_filename in zip(all_hdf_files, all_hdf_files_with_changed_name):
        os.rename(old_filename, new_filename)
        all_hdf_files_with_changed_name.append(new_filename)
    return all_hdf_files_with_changed_name


def _get_random_string(length=10):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str