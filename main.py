import helpers.s3 as s
import helpers.psql as p
import helpers.redshift as r


def main(job):
    try:
        # parameters: s3
        bucket_name = job.s3.bucket_name

        # parameters: redshift
        master_username = job.redshift.master_username
        master_password = job.redshift.master_password
        cluster_identifier = job.redshift.cluster_identifier
        dbname = job.redshift.dbname

        # s3: init
        s3 = s.S3(bucket_name)
        s3.create_bucket(bucket_name)

        # redshift: init
        redshift = r.Redshift()
        redshift.create_cluster(cluster_identifier, dbname, master_username, master_password)
        print("redshift cluster initialized.")

        # redshift: waiting for init
        redshift.waiter(cluster_identifier, 0)
        print("redshift cluster initilalization completed.")

        # redshift: get cluster info
        cluster = redshift.describe_cluster(cluster_identifier)
        print("redshift cluster: %s" % cluster_identifier)

        # from local to upload s3
        for dbname in job.local.databases:
            tables = psql_s3(dbname, bucket_name)
            print("%s processed and uploaded to s3." % dbname)

            s3_redshift(dbname, tables, bucket_name, cluster, master_username, master_password)
            print("from %s, %d tables transferred from s3 to redshift." % (dbname, len(tables)))

        print("success")
    except Exception as e:
        print(e)


def psql_s3(dbname, bucket_name):
    s3 = s.S3(bucket_name)
    s3.create_bucket(bucket_name)
    print("connected to s3 bucket: %s" % bucket_name)

    try:
        # connect to db
        db = p.PSQL(dbname, 'localhost')

        # get tables
        table_list = db.get_tables()

        # processing tables
        for table_name in table_list:
            # write data to csv files
            db.table_to_csv(table_name)

            # s3 upload
            file = open('{}.csv'.format(table_name), 'rb')
            s3.put_object(file, '{}/{}.csv'.format(dbname, table_name))

        return table_list
    except Exception as e:
        print(e)
    finally:
        db.conn.close()


def s3_redshift(dbname, table_list, bucket_name, cluster, master_username, master_password):
    try:
        # connect default database first
        machindw = p.PSQL('dev', cluster['Endpoint']['Address'], '5439', master_username, master_password)
        print("---- connected to database: %s" % 'dev')

        # create database
        databases = machindw.get_databases()
        if (dbname,) not in databases:
            machindw.create_database(dbname)
        machindw.conn.close()

        # drop current connection and create new
        machindw = p.PSQL(dbname, cluster['Endpoint']['Address'], '5439', master_username, master_password)
        print("---- connected to database: %s" % dbname)

        if len(machindw.get_tables()) == 0:
            # create db tables
            machindw.execute_file('resources/{}db.sql'.format(dbname))
            print("---- tables created for: %s" % dbname)

            # copy data
            for table_name in table_list:
                s3_path = 's3://{}/{}/{}.csv'.format(bucket_name, dbname, table_name)
                machindw.copy(table_name, s3_path)
                print("---- data uploaded to: %s" % table_name)

            if dbname == 'zagi':
                # create dw tables
                machindw.execute_file('resources/{}dw.sql'.format(dbname))
                print("---- tables (dw) created for: %s" % dbname)
    except Exception as e:
        print(e)
    finally:
        if machindw.conn is not None:
            machindw.conn.close()
            print("---- connection to database closed.")


def purge_everything(cluster_identifier, bucket_name):
    print("purge started.")
    redshift = r.Redshift()
    redshift.delete_cluster(cluster_identifier)

    redshift.waiter(cluster_identifier, 1)
    print("purged: redshift cluster: %s" % cluster_identifier)

    s3 = s.S3(bucket_name)
    objts = s3.list_objects()
    for obj in objts:
        s3.delete_object(obj['Key'])
        print("purged: s3 object: %s" % obj['Key'])

    s3.delete_bucket(bucket_name)
    print("purged: s3 bucket: %s" % bucket_name)

    print("good bye!")


if __name__ == '__main__':

    from helpers import job as j

    job = j.Job(
        purge=False,
        s3={
            "bucket_name": "",
        },
        redshift={
            "cluster_identifier": "",
            "cluster_name": "",
            "master_username": "",
            "master_password": "",
            "dbname": ""
        },
        local={
            "conn_info":
                {
                    "user": "",
                    "password": "",
                    "host": "",
                    "port": 0
                },
            "databases": [
                "",
            ]
        }
    )

    if job.purge is False:
        main(job)
    else:
        purge_everything(job)
