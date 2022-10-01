import boto3
import time
import json
import pandas as pd
import configparser
import psycopg2
from create_redshift import get_dwh_params,get_redshift_status,create_aws_client
from botocore.exceptions import ClientError
   

def delete_redshift(redshift):
    """ Function to delete a REDSHIFT cluster"""
    """ INPUT redshift client (Boto3 client), roleArn the ARN string for the IAM role """
    """ OUTPUT None (Redshift cluster deleted)"""    

    print("4. Deleting AWS Redshift cluster")
    try:
        # add parameters for identifiers & credentials
     
        response = redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,SkipFinalClusterSnapshot=True)
        
    except Exception as e:
        print(e)


def main():
    """ Main function that orchestrates everything """
    """ This main function reads the configuration file and deletes a redshift cluster """
    
    # get AWS parameters
    get_dwh_params()
    
    # Delete Redshift cluster
    redshift = create_aws_client('redshift',"us-west-2")
    print(redshift.describe_clusters)
    #properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    #print(properties)
    
    if get_redshift_status(redshift)=='available':
        print("Redshift is up...")
        redshift.delete_cluster(ClusterIdentifier=endpoint,SkipFinalClusterSnapshot=True)
        #delete_redshift(redshift)
        print ("Reshift status is ", get_redshift_status(redshift))        
    
    else:
        print("Redshift is not avaiable. An error has ocurred while deleting the Redshift cluster")


if __name__ == "__main__":
    main()