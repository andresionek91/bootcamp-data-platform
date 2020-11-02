import boto3
import json
from fake_web_events import Simulation

client = boto3.client('firehose')


def put_record(event):
    data = (json.dumps(event, ensure_ascii=True).replace('true', '"true"').replace('false', '"false"') + '\n').encode('utf-8')
    print(data)
    response = client.put_record(
        DeliveryStreamName='firehose-production-raw-delivery-stream',
        Record={
            'Data': data
        }
    )
    return response


simulation = Simulation(user_pool_size=100, sessions_per_day=10000)
events = simulation.run(duration_seconds=600)

for event in events:
    put_record(event)