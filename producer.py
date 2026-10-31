import json
from client import kinesis

event = {
    "order_id": "order-003",
    "product": "book",
    "quantity": 1,
}

response = kinesis.put_record(
    StreamName = "orders",
    PartitionKey=event["order_id"],
    Data=json.dumps(event).encode("utf-8"),
)

print(response["ShardId"])
print(response["SequenceNumber"])