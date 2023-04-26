import glob
import lzma
import os
import random
import shutil
import string
import subprocess

from common import mktemp
from converters import Converters

BUCKET = "shieldhit-results-bucket"


def execute(event):
    action = event.get("action", "action not provided")
    if action == "simulate":
        n = event.get("n", 1000)
        N = event.get("N", 0)
        files = event["files"]
        save_to = event.get("save_to", "download")
        loaded = load(files, "uploaded")
        results = run_shieldhit(n, N, loaded)
        return save(results, ".bdo", save_to)
    elif action == "simulate_and_extract":
        n = event.get("n", 1000)
        N = event.get("N", 0)
        files = event["files"]
        save_to = event.get("save_to", "download")
        loaded = load(files, "uploaded")
        results = run_shieldhit(n, N, loaded)
        results = run_convertmc(results, 0)
        return save(results, ".h5", save_to)
    elif action == "extract_and_reduce":
        files = event["files"]
        get_from = event.get("get_from", "uploaded")
        worker_id_prefix = event["N"]
        loaded = load(files, get_from)
        results = run_convertmc(loaded, worker_id_prefix)
        return save(results, ".h5", "download")
    else:
        raise Exception(f"Unknown action: {action}")


def run_shieldhit(n, N, dir):
    try:
        subprocess.check_output(["chmod", "a+x", "shieldhit"])
    except Exception:
        pass

    subprocess.check_output(["./shieldhit", "-n", str(n), "-N", str(N), dir])

    return dir


def run_convertmc(dir, worker_id_prefix):
    try:
        subprocess.check_output(["chmod", "a+x", "convertmc"])
    except Exception:
        pass

    subprocess.run(
        f"./convertmc hdf --many {dir}/*.bdo {dir}",
        check=False,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    all_hdf_files = glob.glob(f"{dir}/*.h5")
    _all_hdf_files_with_changed_name = _rename_hdf_files(
        all_hdf_files, worker_id_prefix
    )
    return dir


def load(files, get_from):
    tmpdir = mktemp()

    if get_from == "uploaded":
        Converters.map_to_files(files, tmpdir, lzma.decompress)
    elif get_from == "s3":
        import boto3

        client = boto3.client("s3")
        for just_filename in files.keys():
            client.download_file(
                BUCKET, files[just_filename], f"{tmpdir}/{just_filename}"
            )
    elif get_from == "volume":
        for just_filename in files.keys():
            shutil.move(files[just_filename], tmpdir)
    return tmpdir


def save(dir, files_to_save_extension, save_to):
    all_result_files = glob.glob(f"{dir}/*{files_to_save_extension}")
    if save_to == "s3":
        import boto3

        bucket_dir = _get_random_string()
        client = boto3.client("s3")
        results_map = {}
        for filename in all_result_files:
            _directory_path, just_file_name = os.path.split(filename)
            f = open(filename, "rb")
            client.put_object(
                Body=f.read(), Bucket=BUCKET, Key=f"{bucket_dir}/{just_file_name}"
            )
            f.close()
            results_map[just_file_name] = f"{bucket_dir}/{just_file_name}"
    elif save_to == "download":
        results_map = Converters.files_to_map(all_result_files, transform=lzma.compress)
    elif (
        (type(save_to) is tuple or type(save_to) is list)
        and len(save_to) == 2
        and save_to[0] == "volume"
    ):
        volume_dir = save_to[1]
        volume_tmp_dir = mktemp(volume_dir)
        results_map = {}
        for filename in all_result_files:
            _directory_path, just_file_name = os.path.split(filename)
            shutil.move(filename, volume_tmp_dir)
            results_map[just_file_name] = f"{volume_tmp_dir}/{just_file_name}"
    else:
        raise Exception(f"Unknown save_to parameter: {save_to}")
    subprocess.check_output(f"rm -r {dir}", shell=True)
    return results_map


def _rename_hdf_files(all_hdf_files, worker_id_prefix=""):
    all_hdf_files_with_changed_name = []
    for filename in all_hdf_files:
        just_filename, extension = os.path.splitext(filename)
        all_hdf_files_with_changed_name.append(
            f"{just_filename}{worker_id_prefix}{extension}"
        )
    for old_filename, new_filename in zip(
        all_hdf_files, all_hdf_files_with_changed_name
    ):
        os.rename(old_filename, new_filename)
        all_hdf_files_with_changed_name.append(new_filename)
    return all_hdf_files_with_changed_name


def _get_random_string(length=10):
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str
