import json, time
from client import kinesis

response = kinesis.get_shard_iterator(
    StreamName="orders",
    ShardId="shardId-000000000000",
    ShardIteratorType="TRIM_HORIZON",
)

iterator = response["ShardIterator"]

while True:
    response = kinesis.get_records(
        ShardIterator=iterator,
        Limit=10,
    )

    for record in response["Records"]:
        event = json.loads(record["Data"].decode("utf-8"))
        print(event)

    iterator = response["NextShardIterator"]
    time.sleep(1)