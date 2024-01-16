import argparse
import configparser
import json
import logging
import boto3
from botocore.exceptions import ClientError


#CONFIG
config = configparser.ConfigParser()
config_file = 'dwh.cfg'
config.read(config_file)

def get_config(filepath=config_file):
    """
    Get config object from config file

    Arg(s):
        filepath: path to the config file
    Return(s):
        config object
    """
    config = configparser.ConfigParser()
    try:
        config.read_file(open(config_file))
    except Exception as e:
        print(e)

    return config



KEY                 = config['AWS']['AWS_ACCESS_KEY_ID']
SECRET              = config['AWS']['AWS_SECRET_ACCESS_KEY']
DWH_IAM_ROLE_NAME   = config['CLUSTER']['DWH_IAM_ROLE_NAME']
DWH_CLUSTER_ID      = config['CLUSTER']['DWH_CLUSTER_IDENTIFIER']
REGION              = config['CLUSTER']['REGION']
DB_PORT             = config['DB']['DB_PORT']
S3_READ_ARN         = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"


def create_resources():
    """ Create required AWS resources """
    options = dict(region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    ec2 = boto3.resource('ec2', **options)
    iam = boto3.client('iam', **options)
    redshift = boto3.client('redshift', **options)
    return ec2, iam, redshift


def create_iam_role(iam):
    """ Create IAM role for Redshift cluster """
    try:
        dwh_role = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                }],
                'Version': '2012-10-17'
            })
        )
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn=S3_READ_ARN
        )
        # print('IAM Role Created: %s.' % (config_file.get('IAM_ROLE', 'arn')))
        print(dwh_role)
    except ClientError as e:
        logging.warning(e)

    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    logging.info('Role {} with arn {}'.format(DWH_IAM_ROLE_NAME, role_arn))
    return role_arn


def create_redshift_cluster(redshift, role_arn):
    """ Create Redshift cluster """
    try:
        response = redshift.create_cluster(
            ClusterType=config['CLUSTER']['DWH_CLUSTER_TYPE'],
            NodeType=config['CLUSTER']['DWH_NODE_TYPE'],
            NumberOfNodes=int(config['CLUSTER']['DWH_NUM_NODES']),
            DBName=config['DB']['DB_NAME'],
            ClusterIdentifier=DWH_CLUSTER_ID,
            MasterUsername=config['DB']['DB_USER'],
            MasterUserPassword=config['DB']['DB_PASSWORD'],
            IamRoles=[role_arn],
           # VpcSecurityGroupIds=[cluster_sg_id]
        )
        logging.info('Creating cluster {}...'.format(DWH_CLUSTER_ID))
        
        # Wait for up to 30 minutes until the cluster is created successfully
        redshift.get_waiter('cluster_available').wait(
            ClusterIdentifier=DWH_CLUSTER_ID,
            WaiterConfig={'Delay': 30, 'MaxAttempts': 60})
        logging.info('Wait for cluster to become available {}...'.format(DWH_CLUSTER_ID))

        return response['Cluster']
    except ClientError as e:
        logging.warning(e)


def delete_iam_role(iam):
    """ Delete IAM role """
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=S3_READ_ARN)
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    logging.info('Deleted role {} with {}'.format(DWH_IAM_ROLE_NAME, role_arn))


def delete_redshift_cluster(redshift):
    """ Delete Redshift cluster """
    try:
        # delete cluster
        print('Deleting Redshift cluster {}. This might take a few minutes ...'.format(config['CLUSTER']['DWH_CLUSTER_IDENTIFIER']))
        response = redshift.delete_cluster( ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'],  
                                            SkipFinalClusterSnapshot=True)
        # Wait for up to 30 minutes until the cluster is deleted successfully
        redshift.get_waiter('cluster_deleted').wait(ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'],
                                                    WaiterConfig={'Delay': 30, 'MaxAttempts': 60})

        logging.info('Deleted cluster {}'.format(DWH_CLUSTER_ID))
    except Exception as e:
        logging.error(e)

def revoke_ingress_rules(ec2):
    try:
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


# def create_cluster_security_group(ec2):
#   """Creates VPC Security Group on AWS

#   Returns:
#       string: Security Group ID
#   """

#   vpc_id = ""
#   if vpc_id == "":
#     response = ec2.describe_vpcs()
#     vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')
#     logging.info('Get public vpc_id: {}'.format(vpc_id))

#   try:
#     response = ec2.create_security_group(GroupName=config.get('SECURITY', 'SG_Name'), 
#                                          Description='Redshift security group', 
#                                          VpcId=vpc_id)
#     security_group_id = response['GroupId']
#     logging.info('Security Group Created: {}'.format(security_group_id))
#     print('Security Group Created %s in vpc %s.' % (security_group_id, vpc_id))

#     ec2.authorize_security_group_ingress(
#         GroupId=security_group_id,
#         IpPermissions=[
#             {'IpProtocol': 'tcp',
#                 'FromPort': 80,
#                 'ToPort': 80,
#                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
#             {'IpProtocol': 'tcp',
#                 'FromPort': 5439,
#                 'ToPort': 5439,
#                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
#         ])
#     return security_group_id
#   except ClientError as e:
#     print(e)



def create_ec2_sg(ec2, config, cluster_props):
    """
    Create a security group for Redshift cluster

    Arg(s):
        ec2: an EC2 resource/client
        config: an object that contains necessary information for setting up the cluster
        cluster_props: an dict that describes the cluster
    Return(s):
        sg: a default security group
    """
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        logging.info('ec2.Vpc {}'.format(cluster_props['VpcId']))
        print(vpc)
        defaultSg = list(vpc.security_groups.all())[0]
        #print(defaultSg)
        # logging.info('Allow TCP connections from {}'.format(defaultSg))
        # logging.info('Allow TCP connections group name {}'.format(defaultSg.group_name))

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0', # allow traffic from any IP source
            IpProtocol='tcp',
            FromPort=int(config['CLUSTER']['CLUSTER_PORT']),
            ToPort=int(config['CLUSTER']['CLUSTER_PORT'])
            )
    except Exception as e:
        print(e)
    # logging.info('Return TCP connections from {}'.format(defaultSg))
    return defaultSg

def update_config_file(config_file, section, key, value):
    """Writes to an existing config file

    Args:
        config_file (ConfigParser object): Configuration file the user wants to update
        section (string): The section on the config file the user wants to write
        key (string): The key the user wants to write
        value (string): The value the user wants to write
    """
    try:
        # Reading cfg file
        config = configparser.ConfigParser()
        config.read(config_file)

        # Setting section, key and value to be write on the .cfg file
        config.set(section, key, value)

        # Writting to cfg file
        with open(config_file, 'w') as f:
            config.write(f)
    except ClientError as e:
        print(f'ERROR: {e}')


def main(args):
    """ Main function """
    ec2, iam, redshift = create_resources()
    if args.delete:
        delete_redshift_cluster(redshift)
        delete_iam_role(iam)
        revoke_ingress_rules(ec2)
        print('Clean up completed. All resources deleted.')
    else:
        role_arn = create_iam_role(iam)
        print(role_arn)
        ##cluster_sg_id = create_cluster_security_group(ec2)
        cluster_props = create_redshift_cluster(redshift, role_arn)#, cluster_sg_id )
        logging.info('cluster created {}'.format(cluster_props))
        cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_ID)['Clusters'][0]
        print(cluster)
       # print('username', config_file['DB']['db_user'])
       # print('password', config_file['DB']['db_password'])
       # print('Cluster Endpoint: {}:{}@{}/{}'.format(config_file['DB']['db_user'], config_file['DB']['db_password'], cluster['Endpoint']['Address'], config_file['DB']['db_name']))
       # print('db name', config_file['DB']['db_name'])
        sg = create_ec2_sg(ec2, config, cluster_props)
        #print('Cluster Setup done.')
        #print('RoleArn: {}'.format(roleArn))
        print('SecurityGroup: {}'.format(sg))
        print(sg)
        print("SecurityGroup's Name: {}".format(sg.group_name))
        print("SecurityGroup's GRoup ID: {}".format(sg.id))
        print("SecurityGroup's IP permissions: {}".format(sg.ip_permissions))
        # Writing to .cfg file
        print('Updatting CFG file...')
        print(f'Cluster created.')
        print(f"Endpoint={cluster['Endpoint']['Address']}")
        
        

        role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        logging.info('Role {} with arn {}'.format(DWH_IAM_ROLE_NAME, role_arn))
        
        update_config_file(config_file, 'DB', 'HOST', cluster['Endpoint']['Address'])
        update_config_file(config_file, 'IAM_ROLE', 'ARN', role_arn)
        #update_config_file(config_file, 'SECURITY', 'SG_NAME', cluster_sg_id)
        print('CFG file Updated.')
        # else:
        #     logging.error('Could not connect to cluster')
        



if __name__ == '__main__':
    """ Set logging level and cli arguments """
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', dest='delete', default=False, action='store_true')
    args = parser.parse_args()
    main(args)