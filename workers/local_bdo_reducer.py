from common import cmd, meassure_time, mktemp, separate_results
import shutil
import glob
import os
from datatypes.filesystem import FilesystemHDF, FilesystemBinary
import functools


def launch_worker(
    input_files: FilesystemBinary, output_dir: str, operation: str = "image"
):
    tmpdir = mktemp()

    separate_results(input_files.get_directory(), tmpdir)
    cumulative_reduce_time = 0
    for subdir in glob.glob(f"{tmpdir}/*"):
        _directory_path, just_file_name = os.path.split(subdir)
        reducer_function = functools.partial(
            __launch_convertmc,
            input_files=FilesystemBinary(subdir),
            output_dir=output_dir,
            operation="hdf",
        )
        reducer_result, reduce_time = meassure_time(
            lambda: __launch_convertmc(subdir, output_dir, operation)
        )
        cumulative_reduce_time += reduce_time
        if subdir.endswith("/z_profile"):
            hdf_sample = reducer_result.to_memory().read("z_profile_.h5")
    return FilesystemHDF(output_dir), cumulative_reduce_time, hdf_sample


def __launch_convertmc(input_dir: str, output_dir: str, operation):
    cmd(f"binaries/convertmc {operation} --many {input_dir}/* {output_dir}")
    return FilesystemHDF(output_dir)
