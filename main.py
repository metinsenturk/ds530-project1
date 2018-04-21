import helpers.dynamodb_helpers as dy
import helpers.psql_helpers as p
from random import randint
import requests
import json


def main():
    dydb_tests()


def psql_tests():
    """
    Not Implemented Yet.
    :return:
    """


def dydb_tests():
    try:
        # init the class
        dydb = dy.dynamodb_helpers()

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


if __name__ == "__main__":
    main()
