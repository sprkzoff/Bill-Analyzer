import boto3
import os
import time


import dotenv
import os
dotenv.load_dotenv('..')

default_region = os.getenv('REGION')
default_ts_format = os.getenv('TIMESTAMP_FORMAT')
default_bucket = os.getenv('BUCKET')



def upload(
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
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name='forecast')

    
    print(boto3.Session().resource('s3').Bucket(bucketName).Object(fileKey).upload_file(filePath))

    datasetImportJobName = 'EP_DSIMPORT_JOB_TARGET'
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

    ds_import_job_arn=ds_import_job_response['DatasetImportJobArn']
    return ds_import_job_arn


def wait(
    importJobArn,
    region # region for data to wait
):
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
    