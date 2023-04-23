import functools
import lzma
from collections import defaultdict

from common import execute_concurrently, meassure_time
from datatypes.in_memory import InMemoryBinary


def _get_default_value_for_metrics_dict():
    return {}

def initialize_metrics():
    return defaultdict(_get_default_value_for_metrics_dict)

def prepare_multiple_simulate_functions(launch_simulate):
    return lambda how_many_samples, how_many_workers, dat_files, should_mapper_produce_hdf, save_to: _launch_multiple_remote_mappers(
        how_many_samples,
        how_many_workers,
        dat_files,
        should_mapper_produce_hdf,
        launch_simulate,
        save_to=save_to,
    )


def _launch_multiple_remote_mappers(
    how_many_samples: int,
    how_many_workers: int,
    dat_files: InMemoryBinary,
    launch_mapper,
    save_to: str,
):
    samples_per_worker = int(how_many_samples / how_many_workers)
    mapper_function = functools.partial(
        launch_mapper,
        how_many_samples=samples_per_worker,
        files=dat_files,
        save_to=save_to,
    )

    mapper_results, map_time = meassure_time(
        lambda: execute_concurrently(mapper_function, how_many_workers)
    )
    successfull_mapper_results = [r for r in mapper_results if r["status"] == "OK"]
    print(f"SUCCESS/ALL: {len(successfull_mapper_results)}/{how_many_workers}")

    mappers_request_times = []
    mappers_simulation_times = []
    in_memory_data_to_return = InMemoryBinary({}, lzma.decompress)
    for r in successfull_mapper_results:
        in_memory_data_to_return.merge(r["files"])
        mappers_request_times.append(r["request_time"])
        mappers_simulation_times.append(r["execution_time"])
    return (
        in_memory_data_to_return,
        map_time,
        mappers_request_times,
        mappers_simulation_times,
    )
