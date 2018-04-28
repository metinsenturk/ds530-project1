# Local PostrgreSQL to AWS Redshift Transfer

The project is copying local databases to a redshift cluster.



## Requirements
In order to run the app, you must have the followings to be installed on your machine.
* A local PostgreSQL database. You can download postgres at [here](https://www.postgresql.org/).
* AWS Command Line Interface. It can be downloadable at [here](https://aws.amazon.com/cli/) or can be installed via **pip**.


``` pip
pip install awscli
```
* AWS Credentials that have the following policies.
    * AmazonS3FullAccess
    * AmazonRedshiftFullAccess

The following is the policies that attached to my user at the time.

![aws policies](https://raw.githubusercontent.com/metinsenturk/ds530-project1/making-reusable/resources/aws-policy.png)

If you already have them installed, you need to configure aws credentials.

``` bash
aws configure
```

![aws configure](https://raw.githubusercontent.com/metinsenturk/ds530-project1/making-reusable/resources/aws-configure.png)

This will pop up the following. The credentials you provide should have the following policies.

## Description

This app connects local database, look for the tables, and save tables in csv format into current directory, and uploads all files in given S3 bucket with respect to databases.

In the second part, it will use Redshift's [copy](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command to copy files in the S3 bucket into redshift cluster.

If no S3 bucket exists under the given name, it will create one and use that bucket. Remember that bucket names are unique in aws wide. Similarly, a redshift cluster will be created with given cluster-identifier and will be used if no cluster exists with given parameter. If given cluster name exists in your aws account, it will connect that and upload local database into that cluster.



## How to Run this App

The following object is the resources one should provide to run the app. For every database, a script to create tables must be provided for redshift in the same index at **scripts** attribute. An example is provided for 'zagi' database. **purge** parameter undo everything if sets to true.

``` python
from helpers import job as j

job = j.Job(
    purge=False,
    s3={
        "bucket_name": "machin-s3-default",
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
                "user": "postgres",
                "password": "",
                "host": "localhost",
                "port": 5432
            },
        "databases": [
            "zagi", ],
        "scripts": [
            "resources/zagidb.sql", ]
    }
)
```

The following is the results I get at the time of my running.

### S3 Cluster

![s3 zagi](https://raw.githubusercontent.com/metinsenturk/ds530-project1/making-reusable/resources/s3-zagi.png)

### Redshift Cluster

![redshift status](https://raw.githubusercontent.com/metinsenturk/ds530-project1/making-reusable/resources/r.png)
### Redshift Queries

![redshift queries](https://raw.githubusercontent.com/metinsenturk/ds530-project1/making-reusable/resources/r-queries.png)

### Redshift Loads

>![redshift loads](https://raw.githubusercontent.com/metinsenturk/ds530-project1/making-reusable/resources/r-tables.png)
