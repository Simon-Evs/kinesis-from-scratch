from client import kinesis

response = kinesis.create_stream(
    StreamName="orders",
    ShardCount=1,
)


print(response["ResponseMetadata"]["HTTPStatusCode"])
