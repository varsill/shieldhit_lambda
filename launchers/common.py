from datatypes.in_memory import InMemoryBinary
import functools
from common import meassure_time, execute_concurrently
import lzma


def prepare_multiple_remote_mappers_function(launch_mapper):
  return lambda how_many_samples, how_many_workers, dat_files, should_mapper_produce_hdf: _launch_multiple_remote_mappers(how_many_samples, how_many_workers, dat_files, should_mapper_produce_hdf, launch_mapper)

def _launch_multiple_remote_mappers(how_many_samples: int, how_many_workers: int, dat_files: InMemoryBinary, should_mapper_produce_hdf: bool, launch_mapper):  
  samples_per_worker = int(how_many_samples / how_many_workers)
  mapper_function = functools.partial(
      launch_mapper,
      how_many_samples=samples_per_worker,
      files=dat_files,
      should_produce_hdf=should_mapper_produce_hdf
  )
  
  mapper_results, map_time = meassure_time(lambda: execute_concurrently(mapper_function, how_many_workers))
  successfull_mapper_results = [r for r in mapper_results if r["status"]=="OK"]
  print(f"SUCCESS/ALL: {len(successfull_mapper_results)}/{how_many_workers}")
  
  workers_times = []

  in_memory_data_to_return = InMemoryBinary({}, lzma.decompress)
  for r in successfull_mapper_results:
      in_memory_data_to_return.merge(r["files"])
      del r["files"]
      workers_times.append(r)
  return in_memory_data_to_return, map_time, workers_times