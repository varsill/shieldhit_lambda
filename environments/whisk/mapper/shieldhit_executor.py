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

    Converters.map_to_files(event["files"], "/tmp", lzma.decompress)

    subprocess.check_output(["/action/shieldhit", "-n", str(n), "-N", str(N), "-b", "/tmp/beam.dat", "-g", "/tmp/geo.dat", "-m", "/tmp/mat.dat", "-d", "/tmp/detect.dat", "/tmp"])

    all_result_files = glob.glob("/tmp/*.bdo")

    result_map = Converters.files_to_map(all_result_files, lzma.compress)
    subprocess.check_output(["rm", "-rf", "/tmp/*.bdo"])
    return result_map
