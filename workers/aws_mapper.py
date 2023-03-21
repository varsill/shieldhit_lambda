import os

def launch_single_worker(
    worker_id: int,
    how_many_samples: int,
    files,
    should_produce_hdf: bool
):
    from workers.common.remote_mapper_invocation_api import send_request_to_remote_mapper
    try:
        return send_request_to_remote_mapper(worker_id, how_many_samples, files, os.getenv("AWS_LAMBDA_URL"), should_produce_hdf)
    except Exception as e:
        print("AWS Mapper launch_worker ERROR", str(e))
        return {"status": "ERROR", "reason": str(e)}

def launch_worker(how_many_samples: int, how_many_workers: int, input_files_dir: str, temporary_results: str, should_mapper_produce_hdf: bool):
    from workers.common.remote_mapper_invocation_api import launch_multiple_remote_mappers
    return launch_multiple_remote_mappers(how_many_samples, how_many_workers, input_files_dir, temporary_results, should_mapper_produce_hdf, launch_single_worker)