import json
from environment_common import execute
import time


def lambda_handler(event, _ctx):
    try:
        start_time = time.time()
        result = execute(json.loads(event["body"]))
        end_time = time.time()
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}
    return {
        "statusCode": 200,
        "body": json.dumps({"files": result, "time": end_time - start_time}),
    }
