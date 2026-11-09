"""Diagnostico sem divulgar credenciais temporarias."""
import boto3
import socket
from botocore.config import Config
from botocore.exceptions import ClientError

for name, action in (
    ('credenciais_jdbc', lambda: boto3.client('redshift').get_cluster_credentials(ClusterIdentifier='aula23-dw', DbUser='awsuser', DbName='aula23')),
    ('data_api', lambda: boto3.client('redshift-data', config=Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 0})).execute_statement(ClusterIdentifier='aula23-dw', DbUser='awsuser', Database='aula23', Sql='SELECT 1')),
):
    try:
        action()
        print(name + ': PERMITIDO', flush=True)
    except ClientError as exc:
        print(name + ': ' + exc.response['Error']['Code'], flush=True)
    except Exception as exc:
        print(name + ': ' + type(exc).__name__ + ': ' + str(exc), flush=True)
print('dns_redshift_data: ' + str(socket.getaddrinfo('redshift-data.us-east-1.amazonaws.com', 443)[0][4][0]), flush=True)
