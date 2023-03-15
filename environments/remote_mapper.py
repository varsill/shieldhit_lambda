import requests
from converters import Converters
import glob
from multiprocessing import Pool
import lzma
import functools
from common import meassure_time
import os
from environments.mapper import Mapper as MapperInterface

class RemoteMapper(MapperInterface):
    """
    Default implementation of a mapper used in FaaS.
    """


    def __init__(
        self,
        how_many_samples: int,
        how_many_workers: int,
        input_files_dir: str,
        output_dir: str,
        should_produce_hdf: bool

    ):
        self.how_many_samples = how_many_samples
        self.how_many_workers = how_many_workers
        self.input_files_dir = input_files_dir
        self.output_dir = output_dir
        self.should_produce_hdf = should_produce_hdf
        self.lambda_url = os.getenv("LAMBDA_URL")

    def execute(self):
        samples_per_worker = int(self.how_many_samples / self.how_many_workers)
        worker = functools.partial(
            _launch_worker,
            how_many_samples=samples_per_worker,
            input_files_dir=self.input_files_dir,
            output_dir=self.output_dir,
            lambda_url=self.lambda_url,
            should_produce_hdf=self.should_produce_hdf
        )

        with Pool(self.how_many_workers) as process:
            results = process.map_async(worker, range(self.how_many_workers))
            results.wait()
        
        results = results.get()
        
        successfull_results = [r for r in results if r["status"]=="OK"]
        
        print(f"SUCCESS/ALL: {len(successfull_results)}/{self.how_many_workers}")
        for result in successfull_results:
            try:
                Converters.map_to_files(result["files"], self.output_dir, lzma.decompress, False)
                del result["files"]
            except Exception as e:
                print("execute ERROR", e)
                
    
        return {"workers_times": results}


def _launch_worker(
    worker_id: int,
    how_many_samples: int,
    input_files_dir: str,
    output_dir: str,
    lambda_url: str,
    should_produce_hdf: bool
):
    try:
        dat_files = Converters.files_to_map(
            glob.glob(f"{input_files_dir}/*"), lzma.compress
        )
        json_input = {"n": how_many_samples, "N": worker_id, "files": dat_files, "should_produce_hdf": should_produce_hdf}
        response, request_time = meassure_time(
            lambda: requests.post(lambda_url, json=json_input)
        )
        os.makedirs(output_dir, exist_ok=True)
        if response.status_code != 200:
            raise Exception(
                f"Worker {worker_id}: reponse unsuccessful! Reason: {response.content}"
            )
       
        result = {
            "status": "OK",
            "worker_id": worker_id,
            "simulation_time": response.json()["time"],
            "request_time": request_time,
            "files": response.json()["files"]
        }
        return result
    except Exception as e:
        print("_launch_worker ERROR", str(e))
        return {"status": "ERROR", "reason": str(e)}
