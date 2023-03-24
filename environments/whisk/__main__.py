import json
from environment_common import execute
import time
import subprocess

def main(event):
    try:
        start_time = time.time()
        #result = execute(event)
        end_time = time.time()
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
    return {
        "statusCode": 200,
        "body": json.dumps({"files": 1, "time": end_time - start_time}),
    }

  