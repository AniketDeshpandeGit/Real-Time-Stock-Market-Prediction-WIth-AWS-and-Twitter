import boto3
import json
from datetime import datetime
import calendar
import random
import time
import requests

my_stream_name = 'stock-data'

kinesis_client = boto3.client('kinesis',
                              aws_access_key_id="AKIAT3PTZM7VCJ6EQ2I5",
                              aws_secret_access_key="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",
                              region_name="us-east-1")

def put_to_stream(payload):
    print(payload)

    put_response = kinesis_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(payload),
                        PartitionKey="XX")

while True:
    results = requests.get('https://api.tiingo.com/iex/amzn/prices?startDate=2019-05-02&resampleFreq=1min&token=6acb967bbf31715249a8ac874ca4c20cd78bc508')
    for result in results.json():
        del result['high']
        del result['open']
        del result['low']
        put_to_stream(result)

    # wait for 5 second
    time.sleep(10800)
