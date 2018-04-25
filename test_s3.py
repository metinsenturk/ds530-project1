import boto3 as boto

# credentials
session = boto.Session()
credentials = session.get_credentials()
credentials = credentials.get_frozen_credentials()

# init aws
client = boto.client('s3')

# create bucket
response = client.create_bucket(
    Bucket='test-bucket'
)

if response['Location']:
    print("success")

# put object in s3
f = open('product.csv', 'rb')
response = client.put_object(
    ACL='private',
    Key='tests/product.csv',
    Bucket='product',
    Body=''
)

if response['RequestCharged']:
    print('success')

# list buckets
response = client.list_buckets()

buckets = response['Buckets']

if len(buckets) > 0:
    for bucket in buckets:
        print(bucket)
