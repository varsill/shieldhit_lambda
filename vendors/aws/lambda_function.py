import json
from shieldhit_executor import *

def lambda_handler(event, ctx):
  try:
    result = simulation(json.loads(event["body"]))
  except Exception as e:
    return {
      'statusCode': 500,
      'body': json.dumps(str(e))
    }
  return {
    'statusCode': 200,
    'body': json.dumps(result)
  }