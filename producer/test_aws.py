import boto3

client = boto3.client(
    "kinesis",
    region_name="ap-south-1"
)

response = client.list_streams()

print(response)