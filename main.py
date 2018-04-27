import helpers.s3 as s
import helpers.psql as p
import helpers.redshift as r

bucket_name = 'machin-ds530'


def main():
    try:
        # tables for databases
        zagi_table_names = ['category', 'customer', 'product', 'region',
                            'salestransaction', 'soldvia', 'store', 'vendor']
        homeaway_table_names = ['apartment', 'building', 'cleaning', 'corpclient',
                                'inspecting', 'inspector', 'manager', 'managerphone', 'staffmember']

        # from local to upload s3
        psql_s3('zagi', zagi_table_names)
        psql_s3('homeaway', homeaway_table_names)

        # parameters for redshift cluster
        master_username = 'machinroot'
        master_password = '367Rabbit'
        cluster_identifier = 'machindw'
        dbname = 'dev'

        # creating cluster if not exists
        redshift = r.Redshift()
        redshift.create_cluster(cluster_identifier, dbname, master_username, master_password)

        # waiting cluster to become available
        redshift.waiter(cluster_identifier, 0)
        cluster = redshift.describe_cluster(cluster_identifier)

        # copying s3 to redshift
        s3_redshift('zagi', zagi_table_names, cluster, master_username, master_password)
        s3_redshift('homeaway', homeaway_table_names, cluster, master_username, master_password)

        print("success")
    except Exception as e:
        print(e)


def psql_s3(dbname, table_list):
    try:
        s3 = s.S3()
        s3.bucket_name = bucket_name
        db = p.PSQL(dbname, 'localhost')

        for table_name in table_list:
            # write data to csv files
            db.table_to_csv(table_name)

            # s3 upload
            file = open('{}.csv'.format(table_name), 'rb')
            s3.put_object(file, '{}/{}.csv'.format(dbname, table_name))
    except Exception as e:
        print(e)
    finally:
        db.conn.close()


def s3_redshift(dbname, table_list, cluster, master_username, master_password):
    try:
        machindw = p.PSQL('dev', cluster['Endpoint']['Address'], '5439', master_username, master_password)

        # create database
        databases = machindw.get_databases()
        if (dbname,) not in databases:
            machindw.create_database(dbname)
        machindw.conn.close()

        # drop current connection and create new
        machindw = p.PSQL(dbname, cluster['Endpoint']['Address'], '5439', master_username, master_password)

        # check tables
        if len(machindw.get_tables()) == 0:
            machindw.execute_file('resources/{}db.sql'.format(dbname))

        # copy data
        for table_name in table_list:
            s3_path = 's3://{}/{}/{}.csv'.format(bucket_name, dbname, table_name)
            machindw.copy(table_name, s3_path)
    except Exception as e:
        print(e)
    finally:
        if machindw.conn is not None:
            machindw.conn.close()


def purge_everything(cluster_identifier, bucket_name):
    redshift = r.Redshift()
    redshift.delete_cluster(cluster_identifier)

    s3 = s.S3()
    objts = s3.list_objects()
    for obj in objts:
        s3.delete_object(obj['Key'])
    s3.delete_bucket(bucket_name)


if __name__ == '__main__':
    purge_everything('machindw', bucket_name)
    # main()
