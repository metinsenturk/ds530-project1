import sys

import boto3 as boto

cluster_identifier = 'machin-rs-test'
master_username = 'rsroot'
master_password = '367Rabbit'
db_user = 'rsuser'

client = boto.client(
    'redshift',
    aws_access_key_id='AKIAJEV3SIIEIH3NMS5A',
    aws_secret_access_key='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'
)

# delete cluster
response = client.delete_cluster(
    ClusterIdentifier=cluster_identifier,
    SkipFinalClusterSnapshot=True
)

cluster = response['Cluster']
client.get_waiter('cluster_deleted').wait(
    ClusterIdentifier=cluster_identifier
)

print("success")

# create cluster
response = client.create_cluster(
    ClusterIdentifier=cluster_identifier,
    NodeType='ds2.xlarge',
    ClusterType='single-node',
    MasterUsername=master_username,
    MasterUserPassword=master_password,
    Port=5439,
    AutomatedSnapshotRetentionPeriod=35,
    PubliclyAccessible=True
)

client.get_waiter('cluster_available').wait(
    ClusterIdentifier=cluster_identifier
)

print("success")

# get credentials
response = client.get_cluster_credentials(
    DbUser=master_username,
    ClusterIdentifier=cluster_identifier,
    DurationSeconds=3600,
    AutoCreate=True
)
