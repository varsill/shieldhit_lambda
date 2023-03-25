import os
from datatypes.in_memory import InMemoryBinary

def launch_worker(
    worker_id: int, how_many_samples: int, files: InMemoryBinary, should_produce_hdf: bool
):
    from workers.common.remote_mapper_invocation_api import (
        send_request_to_remote_mapper,
    )

    try:
        return send_request_to_remote_mapper(
            worker_id,
            how_many_samples,
            files,
        os.getenv("AWS_LAMBDA_URL"),
            should_produce_hdf,
        )
    except Exception as e:
        print("AWS Mapper launch_worker ERROR", str(e))
        return {"status": "ERROR", "reason": str(e)}
