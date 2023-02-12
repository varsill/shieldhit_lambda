import subprocess
import glob
import os
from converters import *
import lzma

def simulation(event): 
    n = event.get("n", 1000)
    N = event.get("N", 0)
    
    try:
        subprocess.check_output(['cp', '-rf', 'shieldhit', '/tmp'])
        subprocess.check_output(['chmod', 'a+x', '/tmp/shieldhit'])
    except Exception:
        pass
    os.chdir("/tmp")
    map_to_files(event["files"], "./", lzma.decompress)

    subprocess.check_output(['./shieldhit', '-n', str(n), '-N', str(N)])
    
    all_result_files = glob.glob("./*.bdo")
  
    result_map = files_to_map(all_result_files, lzma.compress)
    subprocess.check_output(['rm', '-rf', './*.bdo'])
    return result_map
    

    