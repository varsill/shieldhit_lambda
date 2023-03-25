import subprocess
import glob
import os
from common import mktemp
from converters import Converters
import lzma

def execute(event):
    action = event.get("action", "action not provided")
    if action=="map":
        n = event.get("n", 1000)
        N = event.get("N", 0)
        files = event["files"]
        return mapper(n, N, files, False)
    elif action=="map_and_reduce":
        n = event.get("n", 1000)
        N = event.get("N", 0)
        files = event["files"]
        return mapper(n, N, files, True)
    elif action=="reduce":
        files = event["files"]
        operation = event["operation"]
        return reducer(files, operation)
    else:
        raise Exception(f"Unknown action: {action}")
    

def mapper(n, N, files, should_produce_hdf):
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
        all_hdf_files_with_changed_name = _rename_hdf_files(all_hdf_files, str(N))
        result_map = Converters.files_to_map(all_hdf_files_with_changed_name, lzma.compress)
    else:
        all_bdo_files = glob.glob(f"{tmpdir}/*.bdo")
        result_map = Converters.files_to_map(all_bdo_files, lzma.compress)

    subprocess.check_output(["rm", "-rf", tmpdir])
    return result_map

def reducer(files, operation):
    try:
        subprocess.check_output(["chmod", "a+x", "convertmc"])
    except Exception:
        pass
    tmpdir = mktemp()
    Converters.map_to_files(files, tmpdir, lzma.decompress)
    subprocess.check_output(["./convertmc", operation, "--many", f"{tmpdir}/*.bdo", tmpdir])
    if operation == "image":
        extension = ".png"
    elif operation == "hdf":
        extension = ".h5"
    all_hdf_files = glob.glob(f"{tmpdir}/*{extension}")
    result_map = Converters.files_to_map(all_hdf_files, lzma.compress)
    return result_map

def _rename_hdf_files(all_hdf_files, N=""):
    all_hdf_files_with_changed_name = []
    for filename in all_hdf_files:
        just_filename, extension = os.path.splitext(filename)
        all_hdf_files_with_changed_name.append(f"{just_filename}{N}{extension}")
    for old_filename, new_filename in zip(all_hdf_files, all_hdf_files_with_changed_name):
        os.rename(old_filename, new_filename)
        all_hdf_files_with_changed_name.append(new_filename)
    return all_hdf_files_with_changed_name