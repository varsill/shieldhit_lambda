import glob

from common import cmd, meassure_time, mktemp, separate_results
from datatypes.filesystem import FilesystemBinary, FilesystemHDF


def extract_and_reduce(input_files: FilesystemBinary, output_dir: str):
    tmpdir = mktemp()

    separate_results(input_files.get_directory(), tmpdir)
    cumulative_reduce_time = 0
    for subdir in glob.glob(f"{tmpdir}/*"):
        _reducer_result, reduce_time = meassure_time(
            lambda: __launch_convertmc(subdir, output_dir)
        )
        cumulative_reduce_time += reduce_time

    return FilesystemHDF(output_dir), cumulative_reduce_time


def __launch_convertmc(input_dir: str, output_dir: str):
    cmd(f"binaries/convertmc hdf --many {input_dir}/* {output_dir}")
    return FilesystemHDF(output_dir)
