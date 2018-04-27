import boto3 as boto
import botocore as botoc


class S3:
    bucket_name = ''


    def __init__(self, bucket_name):
        # init aws
        client = boto.client('s3')

        self.client = client
        self.bucket_name = bucket_name

    def create_bucket(self, bucket_name):
        client = self.client

        try:
            response = client.head_bucket(
                Bucket=bucket_name
            )
        except botoc.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])

            if error_code == 404:
                response = client.list_buckets()
                buckets = response['Buckets']

                response = client.create_bucket(
                    Bucket=self.bucket_name
                )

            if response['Location']:
                return True
            else:
                return False

    def delete_bucket(self, bucket_name=''):
        client = self.client

        if bucket_name == '':
            bucket_name = self.bucket_name

        client.delete_bucket(
            Bucket=bucket_name
        )

    def list_objects(self, prefix=''):
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
