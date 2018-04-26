import boto3


class Redshift:
    def __init__(self):
        client = boto3.client('redshift')
        self.client = client

    def create_cluster(self, cluster_identifier, master_username, master_password):
        client = self.client

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

        return response['Cluster']

    def delete_cluster(self, cluster_identifier):
        client = self.client

        response = client.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=True
        )

        cluster = response['Cluster']
        client.get_waiter('cluster_deleted').wait(
            ClusterIdentifier=cluster_identifier
        )
        pass
