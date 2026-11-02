import json, time
from client import kinesis
from pathlib import Path
from uuid import uuid4

output_dir = Path("data/batches")
output_dir.mkdir(parents=True, exist_ok=True)
checkpoint_path = Path("data/orders.checkpoint")

if checkpoint_path.exists():
    last_sequence_number = checkpoint_path.read_text(encoding="utf-8").strip()
    response = kinesis.get_shard_iterator(
        StreamName="orders",
        ShardId="shardId-000000000000",
        ShardIteratorType="AFTER_SEQUENCE_NUMBER",
        StartingSequenceNumber=last_sequence_number,
    )
else:
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

    records = response["Records"]

    if records:
        
        batch_path = output_dir / f"orders-{uuid4().hex}.jsonl"

        with batch_path.open("w",encoding="utf-8") as output:
            for record in records:
                event = json.loads(record["Data"].decode("utf-8"))
                output.write(json.dumps(event) + "\n")
                print(event)
        last_sequence_number = records[-1] ["SequenceNumber"]
        checkpoint_path.write_text(last_sequence_number, encoding="utf-8")

    iterator = response["NextShardIterator"]
    time.sleep(1)