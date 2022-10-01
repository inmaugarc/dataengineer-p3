import boto3
import time
import json
import pandas as pd
import configparser
import psycopg2
from botocore.exceptions import ClientError


def get_dwh_params():
    """ Function to load DWH Params from a file:dwh.cfg"""
    """ INPUT None"""
    """ OUTPUT Params to config AWS Redshift Cluster"""
    # define global these variables
    global KEY,SECRET,DWH_IAM_ROLE_NAME
    global DWH_CLUSTER_TYPE,DWH_NUM_NODES,DWH_NODE_TYPE,DWH_CLUSTER_IDENTIFIER
    global DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT
    
    print ("Getting AWS configuration parameters ...\n")
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('IAM_ROLE','key')
    SECRET                 = config.get('IAM_ROLE','secret')
    DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

    DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")

    DWH_DB                 = config.get("DB","DWH_DB")
    DWH_DB_USER            = config.get("DB","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DB","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DB","DWH_PORT")

    print ("----AWS configuration parameters----")
    print ("DWH_IAM_ROLE_NAME:",DWH_IAM_ROLE_NAME)
    print ("DWH_CLUSTER_TYPE:",DWH_CLUSTER_TYPE)
    print ("DWH_NUM_NODES:",DWH_NUM_NODES)
    print ("DWH_NODE_TYPE:",DWH_NODE_TYPE)
    print ("DWH_CLUSTER_IDENTIFIER:",DWH_CLUSTER_IDENTIFIER)
    print ("DWH_DB:",DWH_DB)
    print ("DWH_DB_USER:",DWH_DB_USER)
    print ("DWH_DB_PASSWORD:",DWH_DB_PASSWORD);
    print ("DWH_PORT:",DWH_PORT)
    print ("\n")

    
def create_aws_service(resource_name, region):
    """ Function to create an AWS service in a region"""
    """ INPUT service, region """
    """ OUTPUT created service """
    
    
    # Create an AWS resource in a region with private credentials 
    resource = boto3.resource(resource_name,region_name=region,aws_access_key_id=KEY,aws_secret_access_key=SECRET)
    return resource

def create_aws_client(resource_name, region):
    """ Function to create an AWS client in a region"""
    """ INPUT service, region """
    """ OUTPUT created client """
    
    
    # Create an AWS client in a region with private credentials 
    resource = boto3.client(resource_name,region_name=region,aws_access_key_id=KEY,aws_secret_access_key=SECRET)
    return resource


def create_iam_role(iamrole):
    """ Function to create an IAM role that makes Redshift able to access S3 bucket (ReadOnly)"""    
    """ INPUT iamrole """
    """ OUTPUT IAM role ARN string """
    
    dwhRole=None
    # Create a IAM role 
    # TODO: Create the IAM role
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iamrole.create_role(
        RoleName=DWH_IAM_ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'}),
        Description='This is an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)',)
    
    except Exception as e:
        print(e)
        # if the role already exists, get it!
        dwhRole = iamrole.get_role(RoleName=DWH_IAM_ROLE_NAME)
    
    print("1.2 Attaching Policy")
    dwhRole_pol=iamrole.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    
    print("1.3 Get the IAM role ARN")
    iamrole_arn = iamrole.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    return iamrole_arn
    

def launch_redshift(redshift,roleArn):
    """ Function to launch a REDSHIFT cluster"""
    """ INPUT redshift client (Boto3 client), roleArn the ARN string for the IAM role """
    """ OUTPUT boolean value indicating if the cluster has been created correctly or not"""    
    
    print("2. Launching AWS Redshift cluster")
    
    try:
        response = redshift.create_cluster(        
            # add parameters for hardware
            ClusterType   = DWH_CLUSTER_TYPE,
            NodeType      = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES), 

            # add parameters for identifiers & credentials
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            DBName=DWH_DB,
        
            # add parameter for role (to allow s3 access)
            IamRoles=[roleArn])
        
        redshift_response = response['ResponseMetadata']['HTTPStatusCode']
        print("Starting Redshift cluster ",redshift_response)
    
        if redshift_response==200:
            status=True
        else:
            status=False        
        
        return status
    
    except Exception as e:
        print(e)
        return False
    
def get_redshift_status(redshift):
    """ Function to get the status of a REDSHIFT cluster"""
    """ INPUT redshift client (Boto3 client) """
    """ OUTPUT string indicating the status of the cluster (available, available, prep-for-resize,creating,...)"""   
    
    redshift_check  = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    redshift_status = redshift_check['ClusterStatus']
    
    return (redshift_status)


def prettyRedshiftProps(props):
    """ Function to get the pretty properties of a REDSHIFT cluster"""
    """ INPUT redshift props """
    """ OUTPUT pandas dataframe with the properties"""     
    
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def update_config_file(redshift):
    """ Function to add ARN and Redshift endpoint to the (dwh.cfg) config file"""
    """ INPUT redshift props """
    """ OUTPUT properties added to the config file"""       
    
    # first get the properties of the recently created cluster
    properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    # from the general properties, get the endpoint
    endpoint = properties['Endpoint']['Address']
    # from the general properties, get the RoleArn
    arn = properties['IamRoles'][0]['IamRoleArn']
    
    #update config file with those 2 properties
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    config.set("DB", "ENDPOINT", endpoint)
    config.set("IAM_ROLE", "DWH_ROLE_ARN", arn)

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)
    
    get_dwh_params()
    
def open_ports(ec2,redshift):
    """ Function to open ports of the recently created Redshift cluster"""
    """ INPUT ec2 instance,redshift instance """
    """ OUTPUT ports are opened"""       
    
    try:
        properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]      
        vpc = ec2.Vpc(id=properties['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
    
        defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,  # TODO: fill out
        CidrIp='0.0.0.0/0',  # TODO: fill out
        IpProtocol='TCP',  # TODO: fill out
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
       )
    except Exception as e:
        print(e)
    
    
def main():
    """ Main function that orchestrates everything """
    """ This main function reads the configuration file and create a redshift cluster with an open TCP connection """
    """ And after the Redshift cluster is created, then delete tables and then create a new ones"""

    
    # get AWS parameters
    get_dwh_params()
    iam     =  create_aws_client('iam',"us-west-2")
    s3      =  create_aws_service('s3',"us-west-2")
    ec2     =  create_aws_service('ec2',"us-west-2")
    redshift = create_aws_client('redshift',"us-west-2")
    arn=create_iam_role(iam)
    
    # Launch Redshift cluster
    redshift_ok=launch_redshift(redshift,arn)
    #print("Me ha salido ", redshift_ok)
    #redshift_ok=True
    
    if redshift_ok:
        print("Starting Redshift cluster ....")
        
        while True:
            print("Checking if cluster is available")
            status=get_redshift_status(redshift)
            
            if status=='available':
                #update configuration file with cluster info (ARN and endpoint)
                update_config_file(redshift)
                          
                #open ports
                open_ports(ec2,redshift)
                
                break
            else:
                print("...still in progress...")
            
            time.sleep(20)
        print("Cluster is available!")
        
    
    else:
        print("An error has ocurred while creating the Redshift cluster")


if __name__ == "__main__":
    main()
