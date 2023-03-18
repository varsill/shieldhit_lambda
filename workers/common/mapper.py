import subprocess
import glob
import os
from converters import Converters
import lzma


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
   
    Converters.map_to_files(event["files"], tmpdir, lzma.decompress)
     
    subprocess.check_output(["./shieldhit", "-n", str(n), "-N", str(N), tmpdir])

    all_bdo_files = glob.glob(f"{tmpdir}/*.bdo")
    
    if should_produce_hdf:
        subprocess.check_output(["./convertmc", "hdf", "--many", f"{tmpdir}/*.bdo", tmpdir])
        all_hdf_files = glob.glob(f"{tmpdir}/*.h5")
        result_map = Converters.files_to_map(all_hdf_files, lzma.compress)
    else:
        result_map = Converters.files_to_map(all_bdo_files, lzma.compress)

    subprocess.check_output(["rm", "-rf", tmpdir])
    return result_map

