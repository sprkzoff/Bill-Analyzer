# create dataset group to work with data
# dataset group is like a project, you should create it only once

import sys
import os
import json
import time

import pandas as pd
import boto3
from botocore.exceptions import ClientError

from util import extract_arn_from_error

REGION = 'ap-northeast-1'
BUCKET_NAME = 'bill-analyzer-dataset'
PROJECT = 'bill_analyzer'

DATASET_FREQUENCY = "H"
TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss"


def main():


    print(f'REGION={REGION}\nBUCKET_NAME={BUCKET_NAME}\nPROJECT={PROJECT}')
    # set up client
    session = boto3.Session(region_name=REGION)
    forecast = session.client(service_name='forecast')
    forecastquery = session.client(service_name='forecastquery')


    # ================  create project (dataset group)

    trainDataKey = "bill-data/train.csv"
    datasetName = PROJECT+'_ds'
    datasetGroupName = PROJECT + '_dsg'
    s3DataPath = "s3://" + BUCKET_NAME + "/" + trainDataKey

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
            DataFrequency=DATASET_FREQUENCY,
            Schema=schema
        )
        datasetArn = create_dataset_response['DatasetArm']
        print("Created Dataset ARN: ", datasetGroupArn)
    except Exception as e:
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


    return datasetArn, datasetGroupArn

if __name__ == "__main__":
    print("MAIN")
    main()




