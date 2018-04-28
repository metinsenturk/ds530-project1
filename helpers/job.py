class Job:
    def __init__(self, purge, s3, redshift, local):
        self.purge = purge
        self.s3 = S3(
            s3['bucket_name']
        )
        self.redshift = Redshift(
            redshift['cluster_identifier'],
            redshift['cluster_name'],
            redshift['master_username'],
            redshift['master_password'],
            redshift['dbname']
        )
        self.local = Local(
            local['conn_info'],
            local['databases']
        )


class S3(Job):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name


class Redshift(Job):
    def __init__(self, cluster_identifier, cluster_name, master_username, master_password, dbname):
        self.cluster_identifier = cluster_identifier
        self.cluster_name = cluster_name
        self.master_username = master_username
        self.master_password = master_password
        self.dbname = dbname


class Local(Job):
    def __init__(self, conn_info, databases):
        self.databases = databases
        self.conn_info = ConnInfo(
            conn_info['user'],
            conn_info['password'],
            conn_info['host'],
            conn_info['port']
        )


class ConnInfo(Local):
    def __init__(self, user, password, host, port):
        self.user = user
        self.password = password
        self.host = host
        self.port = port


"""
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
"""
