import json
from mapper import simulation
import time
import subprocess

def main(event):
    try:
        start_time = time.time()
        result = simulation(event)
        end_time = time.time()
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
    return {
        "statusCode": 200,
        "body": json.dumps({"files": result, "time": end_time - start_time}),
    }

  