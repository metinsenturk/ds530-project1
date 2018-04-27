import boto3 as boto
from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher


class S3(object):
    bucket_name = 'machin-ds530'

    def __init__(self):
        # init aws
        client = boto.client('s3')
        self.client = client

    def create_bucket(self, bucket_name):
        client = self.client

        response = client.list_buckets()
        buckets = response['Buckets']

        if bucket_name not in buckets:
            if self.bucket_name != bucket_name:
                response = client.create_bucket(
                    Bucket=bucket_name
                )

                if response['Location']:
                    self.bucket_name = bucket_name
                    return True
                else:
                    return False

    def delete_bucket(self, bucket_name):
        client = self.client

        response = client.delete_bucket(
            Bucket=bucket_name
        )

    def list_objects(self, prefix):
        client = self.client

        response = client.list_objects(
            Bucket=self.bucket_name,
            Prefix=prefix,
        )

        objects = response['Contents']
        if len(objects) > 0:
            return objects

    def put_object(self, file, key):
        client = self.client

        client.put_object(
            Key=key,
            Bucket=self.bucket_name,
            Body=file
        )

    def delete_object(self, key):
        client = self.client

        client.delete_object(
            Key=key,
            Bucket=self.bucket_name
        )
