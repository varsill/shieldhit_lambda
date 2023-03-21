from datatypes.filesystem import FilesystemBinary, FilesystemHDF

import lzma
import functools
from common import meassure_time, execute_concurrently, deserialize

ENVIRONMENT = "whisk"

if ENVIRONMENT=="aws":
  from workers.aws_mapper import launch_worker as launch_mapper
elif ENVIRONMENT=="whisk":
  from workers.whisk_mapper import launch_worker as launch_mapper

def launch_remote_mapper(how_many_samples, how_many_workers, input_files_dir, temporary_results, should_mapper_produce_hdf):  
  samples_per_worker = int(how_many_samples / how_many_workers)

  dat_files = FilesystemBinary(input_files_dir, transform=lzma.compress).to_memory().read_all()
  mapper_function = functools.partial(
      launch_mapper,
      how_many_samples=samples_per_worker,
      files=dat_files,
      should_produce_hdf=should_mapper_produce_hdf
  )
  
  mapper_results, map_time = meassure_time(lambda: execute_concurrently(mapper_function, how_many_workers))
  print(mapper_results)
  successfull_mapper_results = [r for r in mapper_results if r["status"]=="OK"]
  print(f"SUCCESS/ALL: {len(successfull_mapper_results)}/{how_many_workers}")
  
  workers_times = []

  for r in successfull_mapper_results:
      deserialize(r["files"], temporary_results)
      del r["files"]
      workers_times.append(r)

  if should_mapper_produce_hdf:
    return_filesystem = FilesystemHDF
  else:
    return_filesystem = FilesystemBinary
  
  return return_filesystem(temporary_results), map_time, workers_times