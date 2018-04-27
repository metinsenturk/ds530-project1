import boto3
from random import randint


def dydb_tests():
    try:
        # init the class
        dydb = boto3.client('dynamodb')

        # creating a table
        table_name = 'table' + str(randint(0, 99))
        table_created = dydb.create_table(table_name)
        print(table_created)

        # list of tables
        table_list = dydb.list_tables()
        print(table_list)

        # delete a table
        for table in table_list:
            if table != 'users':
                dydb.delete_table(table)
                table_list.remove(table)

        # insert controls
        if not 'users' in table_list:
            dydb.create_table('users')

        # get user items
        items = dydb.scan("users", 10)

        for item in items:
            print(item)

    except Exception as e:
        print("Exception: " + str(e))
