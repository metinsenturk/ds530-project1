import json
from sys import stdin
import boto3 as boto

bucket_name = 'machin-s3-buckets-test'
# init aws
client = boto.client('s3')

# list buckets
response = client.list_buckets()
buckets = response['Buckets']

if len(buckets) > 0:
    for bucket in buckets:
        print(bucket)

# create bucket
for bucket in buckets:
    if bucket['Name'] != 'machin-s3-buckets-test':
        response = client.create_bucket(
            Bucket='machin-s3-buckets-test'
        )

        if response['Location']:
            print("success")

# put object
response = client.put_object(
    Key='tests/product4.csv',
    Bucket=bucket_name,
    Body=open('product.csv', 'rb')
)

# delete object
response = client.delete_object(
    Key='tests/product2.csv',
    Bucket=bucket_name
)

# get object
response = client.get_object(
    Key='tests/product.csv',
    Bucket=bucket_name
)
obj = response['Body'].read()
print((obj).decode('utf-8'))

# list objects
response = client.list_objects(
    Bucket=bucket_name
)
objects = response['Contents']

if len(objects) > 0:
    for obj in objects:
        print(obj)

print("success")
