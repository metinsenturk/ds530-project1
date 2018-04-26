import csv
import psycopg2
import os


class PSQL:
    def __init__(self):
        con_str = "dbname = {} user = {} host = {}".format("zagi", "postgres", "localhost")

        self.conn = psycopg2.connect(con_str)

    def table_to_csv(self, table_name):
        file_name = "{}.csv".format(table_name)
        query = "select * from {};".format(table_name)

        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        os.path.join()

        with open(file_name, 'w') as f:
            writer = csv.writer(f, delimiter=',')
            for row in rows:
                writer.writerow(row)
