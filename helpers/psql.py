import csv
import psycopg2
import boto3


class PSQL:
    def __init__(self, dbname, host="localhost", port="PGSQL_LOCAL_PORT", user="postgres", password=''):

        if host == 'localhost':
            con_str = "dbname='{}' user='postgres' host='localhost'".format(dbname)
            self.conn = psycopg2.connect(con_str)
            self.conn.autocommit = True
        else:
            con_str = "dbname='{}' host='{}' port='{}' user='{}' password='{}'" \
                .format(dbname, host, port, user, password)
            self.conn = psycopg2.connect(con_str)
            self.conn.autocommit = True

    def table_to_csv(self, table_name):
        file_name = "{}.csv".format(table_name)
        query = "select * from {};".format(table_name)

        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        with open(file_name, 'w') as f:
            writer = csv.writer(f, delimiter=',')
            for row in rows:
                writer.writerow(row)

        cursor.close()

        return True

    def version(self):
        cursor = self.conn.cursor()

        cursor.execute("select version();")
        db_version = cursor.fetchone()
        cursor.close()

        return db_version[0]

    def get_databases(self):
        cursor = self.conn.cursor()

        cursor.execute("select datname from pg_database;")
        databases = cursor.fetchall()
        cursor.close()

        return databases

    def execute(self, query):
        cursor = self.conn.cursor()

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        return results

    def execute_file(self, filepath):
        cursor = self.conn.cursor()

        cursor.execute(open(filepath, 'r').read())
        cursor.close()

    def copy(self, table, s3_key):
        cursor = self.conn.cursor()

        creds = boto3.session.Session().get_credentials()
        aws_access_key_id = creds.access_key
        aws_secret_access_key = creds.secret_key

        sql = """copy {}.{} from 's3://machin-ds530/{}'\
                credentials \
                'aws_access_key_id={};aws_secret_access_key={}' \
                DELIMITER ',' ACCEPTINVCHARS EMPTYASNULL ESCAPE COMPUPDATE OFF;commit;""" \
            .format('public', table, s3_key, aws_access_key_id, aws_secret_access_key)

        cursor.execute(sql)
        cursor.close()
