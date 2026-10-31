import json
from client import kinesis
from pathlib import Path

response = kinesis.get_shard_iterator(
    StreamName="orders",
    ShardId="shardId-000000000000",
    ShardIteratorType="TRIM_HORIZON",
)

iterator = response["ShardIterator"]

response = kinesis.get_records(
    ShardIterator=iterator,
    Limit=10,
)

output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

with (output_dir / "orders.jsonl").open("w", encoding="utf-8") as output:
    for record in response["Records"]:
        event = json.loads(record["Data"].decode("utf-8"))
        output.write(json.dumps(event) + "\n")
        print(event)