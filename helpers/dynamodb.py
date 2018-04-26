from random import randint
import boto3
import json
import os

'''
    This script is an implementation of the following link.
    https://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html 
    
    This script requires for DynamoDB Local up and running. Script can be found in 
    ./resources/dynamo.sh
    
    Author: Metin Senturk
'''


class dynamodb_helpers:
    def __init__(self):

        self.conn = boto3.client(
            'dynamodb',
            endpoint_url='http://localhost:8000/'
        )

    def batch_write(self, table_name, items):
        """
        Not implemented yet.
        :param table_name:
        :param items:
        :return: boolean
        """
        dynamodb = self.conn

        response = dynamodb.batch_writer()

        return True

    def insert_item(self, table_name, item):

        dynamodb = self.conn

        response = dynamodb.put_item(
            ReturnConsumedCapacity='TOTAL',
            TableName=table_name,
            Item=item
        )

        if response['ResponseMetaData']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def batch_get_item(self):
        dynamodb = self.conn

        items = dynamodb.batch_get_item()

        return items

    def get_item(self, table_name, item):
        """
        gets an item from given table
        :param table_name:
        :param item:
        :return: json
        """
        if not table_name:
            return False
        if not item:
            return False

        dynamodb = self.conn

        response = dynamodb.get_item(
            TableName=table_name,
            Key=item
        )

        item = response['Item']
        return item

    def delete_item(self, table_name, item_key):

        dynamodb = self.conn
        table = dynamodb.Table(table_name)

        response = table.delete_item(
            Key={item_key['name']: item_key['value']}
        )

        if response['ResponseMetaData']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def scan(self, table_name, cnt):

        dynamodb = self.conn

        response = dynamodb.scan(
            TableName=table_name,
            Limit=cnt,
            Select='ALL_ATTRIBUTES',
        )

        items = response['Items']
        return items

    def list_tables(self):
        """
        list of tables in current db.
        :return: dict.
        """
        dynamodb = self.conn

        response = dynamodb.list_tables(
            Limit=10
        )

        items = response['TableNames']
        return items

    def create_table(self, table_name, read_throughput=5, write_throughput=5):
        """
        creates table in dynamodb.
        :param table_name: required.
        :param hash_name: required.
        :param read_throughput: optional.
        :param write_throughput: optional.
        :return: boolean.
        """
        if not table_name:
            return False

        dynamodb = self.conn

        try:
            response = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'id',
                        'AttributeType': 'S'
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': read_throughput,
                    'WriteCapacityUnits': write_throughput
                }
            )

            if response['TableDescription']['TableStatus'] in ['ACTIVE', 'CREATING']:
                return True
            else:
                return False
        except Exception as e:
            return False

    def delete_table(self, table_name):
        """
        deletes given table.
        :param table_name:
        :return: boolean.
        """

        if not table_name:
            return False

        dynamodb = self.conn

        response = dynamodb.delete_table(
            TableName=table_name
        )

        waiter = dynamodb.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        return True


def unmarshalValue(node, mapAsObject):
    for key, value in node.items():
        if (key == "S" or key == "N"):
            return value
        if (key == "M" or key == "L"):
            if (key == "M"):
                if (mapAsObject):
                    data = {}
                    for key1, value1 in value.items():
                        data[key1] = unmarshalValue(value1, mapAsObject)
                    return data
            data = []
            for item in value:
                data.append(unmarshalValue(item, mapAsObject))
            return data
