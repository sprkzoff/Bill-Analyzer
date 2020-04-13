import boto3
import os
import time


import dotenv
import os
from botocore.exceptions import ClientError
from .util import extract_arn_from_error

dotenv.load_dotenv('..')

default_region = os.getenv('REGION')
default_ts_format = os.getenv('TIMESTAMP_FORMAT')
default_bucket = os.getenv('BUCKET')

def upload(
    datasetImportJobName,
    datasetGroupArn,
    datasetArn,
    roleArn,
    # related to uploade file
    filePath,
    fileKey,
    bucketName=default_bucket,
    region=default_region, # region of Forecast
    timestampFormat=default_ts_format
):
    print("="*10, "Creating Dataset import job", "="*10)
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name='forecast')

    
    boto3.Session().resource('s3').Bucket(bucketName).Object(fileKey).upload_file(filePath)
    try:
        ds_import_job_response = forecast.create_dataset_import_job(
            DatasetImportJobName=datasetImportJobName,
            DatasetArn=datasetArn,
            DataSource={
                "S3Config": {
                    "Path": "s3://{}/{}".format(bucketName, fileKey),
                    "RoleArn": roleArn
                }
            },
            TimestampFormat=timestampFormat
        )

        importJobArn=ds_import_job_response['DatasetImportJobArn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            importJobArn = extract_arn_from_error(e)
            print("Data set import job ARN: {} already exists, ignoring".format(importJobArn))
        else:
            print("Unexpected error:", e)
            raise e

    return importJobArn


def wait(
    importJobArn,
    region # region for data to wait
):
    print("="*10, "Waiting Dataset import job to be complete", "="*10)
    lastStatus = None
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name='forecast')
    while True:
        status = forecast.describe_dataset_import_job(DatasetImportJobArn=importJobArn)['Status']
        if lastStatus != status:
            print("\n" + status, end="")
            lastStatus = status
        else:
            print(".", end="")
        if status in ('ACTIVE', 'CREATE_FAILED'): break
        time.sleep(10)
    
    print("Result:", status)
    