import os

def launch_worker(files, operation, get_from, worker_id_prefix=""):
    from workers.common.remote_reducer_invocation_api import send_request_to_remote_reducer
    
    reducer_results = send_request_to_remote_reducer(
        files, operation, os.getenv("WHISK_ACTION_URL"), worker_id_prefix, get_from
    )
    return (
        reducer_results["files"],
        reducer_results["simulation_time"],
        reducer_results["request_time"],
    )
