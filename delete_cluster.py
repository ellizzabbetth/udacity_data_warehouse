import logging
from create_cluster import get_config, create_resources


def main():

    # parse config file
    config = get_config()
    
    region = config['CLUSTER']['REGION']
    id = config['CLUSTER']['DWH_CLUSTER_IDENTIFIER']
    role = config['CLUSTER']['DWH_IAM_ROLE_NAME']
    port = config['CLUSTER']['CLUSTER_PORT']
    # Create resources & clients
    ec2, iam, redshift = create_resources()
    try:
        # delete cluster
        logging.info('Deleting Redshift cluster {}. This might take a few minutes ...'.format(id))
        response = redshift.delete_cluster( ClusterIdentifier=id,  
                                            SkipFinalClusterSnapshot=True)
        # Wait for up to 30 minutes until the cluster is deleted successfully
        redshift.get_waiter('cluster_deleted').wait(ClusterIdentifier=id,
                                                    WaiterConfig={'Delay': 30, 'MaxAttempts': 60})

        # Delete IAM role and attached policy
        logging.info('Deleting IAM Role {}'.format(role))
        iam.detach_role_policy(RoleName=role,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=role)

        # revoke ingress rules
        sg = list(ec2.security_groups.all())[0]
        logging.info('Revoking Ingress rules for SecurityGroup {}'.format(sg))
        sg.revoke_ingress(GroupName=sg.group_name,
                          CidrIp='0.0.0.0/0',
                          IpProtocol='tcp',
                          FromPort=int(port),
                          ToPort=int(port))
    except Exception as e:
        print(e)

    print('Clean up complete. All resources deleted.')

if __name__ == "__main__":
    main()