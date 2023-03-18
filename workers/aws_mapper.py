from workers.common.remote_mapper_invocation_api import invoke_remote_mapper
import os

def launch_worker(
    worker_id: int,
    how_many_samples: int,
    files,
    should_produce_hdf: bool
):
    try:
        return invoke_remote_mapper(worker_id, how_many_samples, files, os.getenv("AWS_LAMBDA_URL"), should_produce_hdf)
    except Exception as e:
        print("AWS Mapper launch_worker ERROR", str(e))
        return {"status": "ERROR", "reason": str(e)}
