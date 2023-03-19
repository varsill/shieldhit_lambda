from common import cmd
import shutil
import glob
import os
from datatypes.filesystem import FilesystemHDF

def launch_worker(input_files: str, output_dir: str, operation: str = "image"):
    cmd(
        f"binaries/convertmc {operation} --many {input_files.get_directory()}/* {output_dir}"
    )
    return FilesystemHDF(output_dir)
