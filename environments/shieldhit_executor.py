import subprocess
import glob
import os
from converters import Converters
import lzma


def simulation(event):
    
    n = event.get("n", 1000)
    N = event.get("N", 0)

    try:
        subprocess.check_output(["chmod", "a+x", "shieldhit"])
    except Exception:
        pass

    tmpdir = subprocess.check_output(["mktemp", "-d"]).decode()[:-1]
    
    Converters.map_to_files(event["files"], tmpdir, lzma.decompress)
     
    subprocess.check_output(["shieldhit", "-n", str(n), "-N", str(N), tmpdir])

    all_result_files = glob.glob(f"{tmpdir}/*.bdo")

    result_map = Converters.files_to_map(all_result_files, lzma.compress)
    subprocess.check_output(["rm", "-rf", tmpdir])
    return result_map
