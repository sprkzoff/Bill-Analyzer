import boto3
import time
from botocore.exceptions import ClientError
from .util import wait

def create(
    region,
    forecastName, # unique name of forecast
    predictorArn,
):
    print("="*10, "Creating Forecast", "="*10)
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name='forecast')
    forecastquery = session.client(service_name='forecastquery')

    createForecastResponse = forecast.create_forecast(
        ForecastName=forecastName,
        PredictorArn=predictorArn
    )
    forecastArn = createForecastResponse['ForecastArn']
    return forecastArn

def wait_create(
    region,
    forecastArn
):
    print("="*10, "Waiting for forecast to be created", "="*10)
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name='forecast')

    wait(what='forecast to be created', statusFunc= lambda:forecast.describe_forecast(
        ForecastArn=forecastArn,
    )['Status'])
    
def query(
    region,
    forecastArn,
    filters
):
    session = boto3.Session(region_name=region)
    forecastquery = session.client(service_name='forecastquery')

    return forecastquery.query_forecast(
        ForecastArn=forecastArn,
        Filters=filter,
    )

def export(
    region,
    jobName,
    forecastArn,
    s3exportPath, # s3 path to file
    exporterRoleArn, # ARN for role which has permission to export
):
    session = boto3.Session(region_name=region)
    client = session.client(service_name="forecast")

    response = client.create_forecast_export_job(
        ForecastExportJobName=jobName,
        ForecastArn=forecastArn,
        Destination={
            'S3Config': {
                'Path': s3exportPath,
                'RoleArn': exporterRoleArn,
            }
        }
    )
    exportJobArn = response['ForecastExportJobArn']
    return exportJobArn
