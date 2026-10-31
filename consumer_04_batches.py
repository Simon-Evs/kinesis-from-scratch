import json, time
from client import kinesis
from pathlib import Path
from uuid import uuid4

response = kinesis.get_shard_iterator(
    StreamName="orders",
    ShardId="shardId-000000000000",
    ShardIteratorType="TRIM_HORIZON",
)

iterator = response["ShardIterator"]

output_dir = Path("data/batches")
output_dir.mkdir(parents=True, exist_ok=True)

while True:
    response = kinesis.get_records(
        ShardIterator=iterator,
        Limit=10,
    )

    records = response["Records"]

    if records:
        
        batch_path = output_dir / f"orders-{uuid4().hex}.jsonl"

        with batch_path.open("w",encoding="utf-8") as output:
            for record in records:
                event = json.loads(record["Data"].decode("utf-8"))
                output.write(json.dumps(event) + "\n")
                print(event)

    iterator = response["NextShardIterator"]
    time.sleep(1)