import subprocess
import glob
import os
from common import serialize, deserialize

def simulation(event):
    n = event.get("n", 1000)
    N = event.get("N", 0)
    should_produce_hdf = event.get("should_produce_hdf", False)
    
    try:
        subprocess.check_output(["chmod", "a+x", "shieldhit"])
        if should_produce_hdf:
            subprocess.check_output(["chmod", "a+x", "convertmc"])
    except Exception:
        pass
    
    tmpdir = subprocess.check_output(["mktemp", "-d"]).decode()[:-1]
   
    deserialize(event["files"], tmpdir)
     
    subprocess.check_output(["./shieldhit", "-n", str(n), "-N", str(N), tmpdir])

    all_bdo_files = glob.glob(f"{tmpdir}/*.bdo")
    
    if should_produce_hdf:
        try:
            subprocess.check_output(["./convertmc", "hdf", "--many", f"{tmpdir}/*.bdo", tmpdir])
        except subprocess.CalledProcessError as e:
            return {"subprocReturnCode": e.returncode, "output": str(e.output)}
        all_hdf_files = glob.glob(f"{tmpdir}/*.h5")
        all_hdf_files_with_changed_name = []
        for filename in all_hdf_files:
            just_filename, extension = os.path.splitext(filename)
            all_hdf_files_with_changed_name.append(f"{just_filename}{N}{extension}")
        for old_filename, new_filename in zip(all_hdf_files, all_hdf_files_with_changed_name):
            os.rename(old_filename, new_filename)
        result_map = serialize(all_hdf_files_with_changed_name)
    else:
        result_map = serialize(all_bdo_files)

    subprocess.check_output(["rm", "-rf", tmpdir])
    return result_map

