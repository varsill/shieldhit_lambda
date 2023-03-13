import requests
from converters import Converters
import glob
from multiprocessing import Pool
import lzma
import functools
from common import meassure_time
import os
from environments.mapper import Mapper as MapperInterface


class Mapper(MapperInterface):
    """
    Module used for samples generation with HPC Whisk environment. Implements 'environment.mapper.Mapper' interface.
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
        self.lambda_url = "https://172.17.0.1:10001/api/v1/web/guest/default/shieldHit"

    def execute(self):
        samples_per_worker = int(self.how_many_samples / self.how_many_workers)
        worker = functools.partial(
            _launch_worker,
            how_many_samples=samples_per_worker,
            input_files_dir=self.input_files_dir,
            output_dir=self.output_dir,
            lambda_url=self.lambda_url,
        )

        with Pool(self.how_many_workers) as process:
            results = process.map_async(worker, range(self.how_many_workers))
            results.wait()
        
        results = results.get()
        how_many_successfully_ended = len([r for r in results if r["status"]=="OK"])
        print(f"SUCCESS/ALL: {how_many_successfully_ended}/{self.how_many_workers}")
        return {"workers_times": results}


def _launch_worker(
    worker_id: int,
    how_many_samples: int,
    input_files_dir: str,
    output_dir: str,
    lambda_url: str,
):
    try:
        dat_files = Converters.files_to_map(
            glob.glob(f"{input_files_dir}/*"), lzma.compress
        )
        json_input = {"n": how_many_samples, "N": worker_id, "files": dat_files}
        response, request_time = meassure_time(
            lambda: requests.post(lambda_url, json=json_input, verify=False)
        )
        os.makedirs(output_dir, exist_ok=True)

        if response.status_code != 200:
            raise Exception(
                f"Worker {worker_id}: response unsuccessful! Reason: {response.status_code} {response.content}"
            )
        Converters.map_to_files(response.json()["files"], output_dir, lzma.decompress)
        metrics = {
            "status": "OK",
            "worker_id": worker_id,
            "simulation_time": response.json()["time"],
            "request_time": request_time,
        }
        return metrics
    except Exception as e:
        print("ERROR", str(e))
        return {"status": "ERROR", "reason": str(e)}
