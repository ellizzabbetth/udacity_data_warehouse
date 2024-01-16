import configparser
import boto3
import json
from create_cluster import get_config, create_resources


def main():

    # parse config file
    config = get_config()
    region = config['CLUSTER']['REGION']
    print(region)
    id = config['CLUSTER']['DWH_CLUSTER_IDENTIFIER']
    print(id)
    role = config['CLUSTER']['DWH_IAM_ROLE_NAME']
    print(role)
    # create resources/clients
    ec2, iam, redshift = create_resources()
    try:
        # delete cluster
        print('Deleting Redshift cluster {}. This might take a few minutes ...'.format(config['CLUSTER']['DWH_CLUSTER_IDENTIFIER']))
        response = redshift.delete_cluster( ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'],  
                                            SkipFinalClusterSnapshot=True)
        # Wait for up to 30 minutes until the cluster is deleted successfully
        redshift.get_waiter('cluster_deleted').wait(ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'],
                                                    WaiterConfig={'Delay': 30, 'MaxAttempts': 60})

        # delete role and attached policy
        print('Deleting IAM Role {}'.format(config['CLUSTER']['DWH_IAM_ROLE_NAME']))
        iam.detach_role_policy(RoleName=config['CLUSTER']['DWH_IAM_ROLE_NAME'],
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=config['CLUSTER']['DWH_IAM_ROLE_NAME'])

        # revoke ingress rules
        sg = list(ec2.security_groups.all())[0]
        print('Revoking Ingress rules for SecurityGroup {}'.format(sg))
        sg.revoke_ingress(GroupName=sg.group_name,
                          CidrIp='0.0.0.0/0',
                          IpProtocol='tcp',
                          FromPort=int(config['CLUSTER']['CLUSTER_PORT']),
                          ToPort=int(config['CLUSTER']['CLUSTER_PORT']))
    except Exception as e:
        print(e)

    print('Clean up complete. All resources deleted.')

if __name__ == "__main__":
    main()