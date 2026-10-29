import boto3

kinesis = boto3.client(
    "kinesis",
    endpoint_url="http://kinesis:4568",
    region_name="us-east-1",
    aws_access_key_id="local",
    aws_secret_access_key="local",

)