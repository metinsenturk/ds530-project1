import boto3


class Redshift:
    def __init__(self):
        client = boto3.client('redshift')
        self.client = client

    def describe_clusters(self, cluster_identifier):
        client = self.client

        response = client.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )

        return response['Clusters'][0]

    def create_cluster(self, cluster_identifier, db_name, master_username, master_password):
        client = self.client

        response = client.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )

        if response['Clusters'][0]['ClusterIdentifier'] != cluster_identifier:
            # create cluster
            response = client.create_cluster(
                DBName=db_name,
                ClusterIdentifier=cluster_identifier,
                NodeType='ds2.xlarge',
                ClusterType='single-node',
                MasterUsername=master_username,
                MasterUserPassword=master_password,
                Port=5439,
                AutomatedSnapshotRetentionPeriod=35,
                PubliclyAccessible=True
            )

            return response['Cluster']

        return response['Clusters'][0]

    def delete_cluster(self, cluster_identifier):
        client = self.client

        response = client.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=True
        )

        cluster = response['Cluster']

        return cluster

    def waiter(self, cluster_identifier, waiter_no):
        client = self.client

        waiter_names = ['cluster_available', 'cluster_deleted']

        client.get_waiter(waiter_names[waiter_no]).wait(
            ClusterIdentifier=cluster_identifier
        )
