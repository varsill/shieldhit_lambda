from datatypes.in_memory import InMemoryBinary
import functools
from common import meassure_time, execute_concurrently
import lzma


def prepare_multiple_remote_mappers_function(launch_mapper):
    return lambda how_many_samples, how_many_workers, dat_files, should_mapper_produce_hdf, save_to: _launch_multiple_remote_mappers(
        how_many_samples,
        how_many_workers,
        dat_files,
        should_mapper_produce_hdf,
        launch_mapper,
        save_to=save_to,
    )


def _launch_multiple_remote_mappers(
    how_many_samples: int,
    how_many_workers: int,
    dat_files: InMemoryBinary,
    should_mapper_produce_hdf: bool,
    launch_mapper,
    save_to: str,
):
    samples_per_worker = int(how_many_samples / how_many_workers)
    mapper_function = functools.partial(
        launch_mapper,
        how_many_samples=samples_per_worker,
        files=dat_files,
        should_produce_hdf=should_mapper_produce_hdf,
        save_to=save_to,
    )

    mapper_results, map_time = meassure_time(
        lambda: execute_concurrently(mapper_function, how_many_workers)
    )
    successfull_mapper_results = [r for r in mapper_results if r["status"] == "OK"]
    print(f"SUCCESS/ALL: {len(successfull_mapper_results)}/{how_many_workers}")

    mappers_times = []

    in_memory_data_to_return = InMemoryBinary({}, lzma.decompress)
    for r in successfull_mapper_results:
        in_memory_data_to_return.merge(r["files"])
        del r["files"]
        mappers_times.append(r)
    return in_memory_data_to_return, map_time, mappers_times
