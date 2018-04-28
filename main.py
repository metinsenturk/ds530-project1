import helpers.s3 as s
import helpers.psql as p
import helpers.redshift as r
import time as t


def create(_job):
    try:
        # parameters: s3
        bucket_name = _job.s3.bucket_name

        # parameters: redshift
        master_username = _job.redshift.master_username
        master_password = _job.redshift.master_password
        cluster_identifier = _job.redshift.cluster_identifier
        dbname = _job.redshift.dbname

        # s3: init
        s3 = s.S3(bucket_name)
        s3.create_bucket(bucket_name)
        print("connected to s3 bucket: %s" % bucket_name)

        # redshift: init
        redshift = r.Redshift()
        redshift.create_cluster(cluster_identifier, dbname, master_username, master_password)
        print("redshift cluster initialized.")

        # redshift: start of creation
        time_start = t.time()
        print("redshift cluster creating: %d" % time_start)

        # redshift: waiting for init
        redshift.waiter(cluster_identifier, 0)
        print("redshift cluster initilalization completed.")

        # redshift: end of creation
        time_end = t.time()
        print("redshift cluster creating: %d" % time_end)

        # redshift: time of creation
        print("waiting time for creation: %d secondes." % (time_end - time_start))

        # redshift: get cluster info
        cluster = redshift.describe_cluster(cluster_identifier)
        print("redshift cluster: %s" % cluster_identifier)

        # from local to upload s3
        for dbname in job.local.databases:
            # Local to S3 ==========================================
            # connect to db
            local = p.PSQL(dbname, 'localhost')

            # get tables
            table_list = local.get_tables()

            # processing tables
            for table_name in table_list:
                # write data to csv files
                local.table_to_csv(table_name)

                # s3 upload
                file = open('{}.csv'.format(table_name), 'rb')
                s3.put_object(file, '{}/{}.csv'.format(dbname, table_name))

            # close connection
            local.conn.close()

            # S3 to Redshift ========================================
            # connect default database first
            redshift_dev = p.PSQL(
                'dev',
                cluster['Endpoint']['Address'],
                cluster['Endpoint']['Port'],
                master_username,
                master_password
            )
            print("---- connected to database: %s" % 'dev')

            # create database
            databases = redshift_dev.get_databases()
            if (dbname,) not in databases:
                redshift_dev.create_database(dbname)

            # close connection
            redshift_dev.conn.close()

            # connect to created database
            redshift_db = p.PSQL(
                dbname,
                cluster['Endpoint']['Address'],
                cluster['Endpoint']['Port'],
                master_username,
                master_password
            )

            print("---- connected to database: %s" % dbname)

            # create db tables
            redshift_db.execute_file('resources/{}db.sql'.format(dbname))
            print("---- tables created for: %s" % dbname)

            # get tables
            table_list = redshift_db.get_tables()

            # copy data
            for table_name in table_list:
                # s3 path
                s3_path = 's3://{}/{}/{}.csv'.format(bucket_name, dbname, table_name)

                # copy s3 to redshift
                redshift_db.copy(table_name, s3_path)
                print("---- data uploaded to: %s" % table_name)

            # close connection
            redshift_db.conn.close()

        print("yeyy")
    except Exception as e:
        print(e)


def purge(cluster_identifier, bucket_name):
    try:
        print("purge started.")

        # init: redshift
        redshift = r.Redshift()

        # init: s3
        s3 = s.S3(bucket_name)

        # delete redshift
        redshift.delete_cluster(cluster_identifier)

        # delete s3 objects
        objts = s3.list_objects()
        for obj in objts:
            s3.delete_object(obj['Key'])
            print("purged: s3 object: %s" % obj['Key'])

        # delete s3 bucket
        s3.delete_bucket(bucket_name)
        print("purged: s3 bucket: %s" % bucket_name)

        # wait for redshift delete
        time_start = t.time()
        print("redshift cluster deletion started: %d" % time_start)

        # waiter
        redshift.waiter(cluster_identifier, 1)
        print("purged: redshift cluster: %s" % cluster_identifier)

        # end time
        time_end = t.time()
        print("redshift cluster deleted: %d" % time_end)

        # time of deletion
        print("waiting time for deletion: %d secondes."(time_end - time_start))

        print("good bye!")
    except Exception as e:
        print(e)


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
        create(job)
    else:
        purge(job)
