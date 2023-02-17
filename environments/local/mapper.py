from converters import *
from multiprocessing import Pool
import functools
from common import meassure_time, cmd
from environments.mapper import Mapper as MapperInterface
import subprocess


class Mapper(MapperInterface):
    """
    Module used for samples generation with local environment. Implements 'environment.mapper.Mapper' interface.
    """

    def __init__(
        self,
        how_many_samples: int,
        how_many_workers: int,
        input_files_dir: str,
        output_dir: str,
    ):
        self.how_many_samples = how_many_samples
        self.how_many_workers = how_many_workers
        self.input_files_dir = input_files_dir
        self.output_dir = output_dir

    def execute(self):
        samples_per_worker = int(self.how_many_samples / self.how_many_workers)
        worker = functools.partial(
            launch_worker,
            how_many_samples=samples_per_worker,
            input_files_dir=self.input_files_dir,
            output_dir=self.output_dir,
        )

        with Pool(self.how_many_workers) as process:
            result = process.map_async(worker, range(self.how_many_workers))
            result.wait()
        return {"workers_times": result.get()}


def launch_worker(worker_id, how_many_samples, input_files_dir, output_dir):
    root_dir = os.getcwd().replace(os.sep, '/')
    input_files_dir = f"{root_dir}/{input_files_dir}"
    output_dir = f"{root_dir}/{output_dir}"
    binaries_dir = f"{root_dir}/binaries"
    mapper_dir = f"{root_dir}/environments/local/mapper"
    try:
        os.makedirs(output_dir, exist_ok=True)
        _result, time = meassure_time(
            lambda: cmd(
                f"docker run \
                -v {input_files_dir}:/home/input \
                -v {binaries_dir}:/home/binaries \
                -v {output_dir}:/home/results \
                -v {mapper_dir}:/home/mapper \
                amazonlinux sh /home/mapper/mapper_script.sh {how_many_samples} {worker_id} \
                ")
        )
        metrics = {
            "status": "OK",
            "worker_id": worker_id,
            "simulation_time": time,
            "request_time": time,
        }
        return metrics
    except Exception as e:
        print("ERROR", str(e))
        return {"status": "ERROR", "reason": str(e)}
