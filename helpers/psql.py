import sys
import psycopg2


class psql_helpers:
    def __init__(self):
        con_str = "dbname ='zagi' " \
                  "user = 'postgres' " \
                  "host = 'localhost'"

        self.conn = psycopg2.connect(con_str)

        cursor = self.conn.cursor()

        version = cursor.execute('select version()')

        print(version)

    def create_table(self, query):
        cursor = self.conn.cursor()

        cursor.execute("select * from product")
        rows = cursor.fetchall()

        for row in rows:
            print(row)
