import os

from datatypes.in_memory import InMemoryBinary

def simulate_and_extract(
    worker_id: int,
    how_many_samples: int,
    files: InMemoryBinary,
    save_to: str,
):

    try:
        from workers.common.remote_invocation_api import send_request_to_remote_simulate_and_extract
        return send_request_to_remote_simulate_and_extract(
            worker_id,
            how_many_samples,
            files,
            os.getenv("WHISK_ACTION_URL"),
            save_to,
        )
    except Exception as e:
        print("Whisk Mapper launch_single_worker ERROR", str(e))
        return {"status": "ERROR", "reason": str(e)}
