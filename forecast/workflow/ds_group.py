# create dataset group to work with data
# dataset group is like a project, you should create it only once

import sys
import os
import json
import time

import pandas as pd
import boto3
from botocore.exceptions import ClientError

from .util import extract_arn_from_error

from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')
default_ds_freq = os.getenv('DATASET_FREQUENCY')
print("default ds freq is", default_ds_freq)

def create(region, bucketName, project, dataset_frequency=default_ds_freq):
    if region is None or bucketName is None or project is None:
        print("Please specify region, bucketName, project")
        return

    print(f'REGION={region}\nbucketName={bucketName}\nproject={project}')
    # set up client
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name='forecast')


    # ================  create project (dataset group)
    datasetName = project+'_ds'
    datasetGroupName = project + '_dsg'

    print("Creating dataset group")
    # create dataset group and get it's ARN
    try:
        create_dataset_group_response = forecast.create_dataset_group(
            DatasetGroupName=datasetGroupName,
            Domain="CUSTOM",
        )
        datasetGroupArn = create_dataset_group_response['DatasetGroupArn']
        print("Created Dataset Group ARN: ", datasetGroupArn)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            datasetGroupArn = extract_arn_from_error(e)
            print("Dataset Group ARN: {} already exists, ignoring".format(datasetGroupArn))
        else:
            print("Unexpected error:", e)
            raise e
    # ================= create dataset 

    # make sure this schema match data's 
    schema = {
        "Attributes": [
            {
                "AttributeName": "timestamp",
                "AttributeType": "timestamp"
            },
            {
                "AttributeName": "target_value",
                "AttributeType": "float"
            },
            {
                "AttributeName": "item_id",
                "AttributeType": "string"
            }
        ]
    }

    # create dataset
    try:
        create_dataset_response = forecast.create_dataset(
            Domain="CUSTOM",
            DatasetType="TARGET_TIME_SERIES",
            DatasetName=datasetName,
            DataFrequency=dataset_frequency,
            Schema=schema
        )
        datasetArn = create_dataset_response['DatasetArm']
        print("Created Dataset ARN: ", datasetGroupArn)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            datasetArn = extract_arn_from_error(e)
            print("Dataset ARN: {} already exists, ignoring".format(datasetArn))
        else:
            print("Unexpected error:", e)
            raise e
    
    # add dataset to dataset group (NO DATA YET)
    forecast.update_dataset_group(
        DatasetGroupArn=datasetGroupArn,
        DatasetArns=[datasetArn]
    )

    return datasetGroupArn, datasetArn





