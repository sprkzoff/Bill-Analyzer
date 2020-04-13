import dotenv
import os
import boto3
from datetime import datetime

from workflow import ds_group, forecast_role, upload_dataset, predictor, forecast
from workflow.util import wait

dotenv.load_dotenv()


def main():
    REGION = os.getenv('REGION')
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    PROJECT = os.getenv('PROJECT')
    EXPORTER_ROLE_ARN = os.getenv('EXPORTER_ROLE_ARN')

    # clients
    session = boto3.Session(region_name=REGION)
    forecastClnt = session.client(service_name='forecast')

    # ===

    # dsgArn, dsArn = ds_group.create(
    #     region=REGION, bucketName=BUCKET_NAME, project=PROJECT   
    # )
    # roleArn = forecast_role.create(wait=5)
    # importJobArn = upload_dataset.upload(
    #     datasetImportJobName="COLD_DATA_IMPORT",
    #     datasetGroupArn=dsgArn,
    #     datasetArn=dsArn,
    #     roleArn=roleArn,
    #     bucketName=BUCKET_NAME,
    #     # filePath="./item-demand-time-train.csv",
    #     filePath="./cold-data-set.csv",
    #     fileKey="bill-analyzer-data/cold.csv",
    # )
    # upload_dataset.wait(importJobArn, region=REGION)

    # # if predictor name is duplicate, then it's not created
    # predictorName='{}_deep_arp_algo'.format(PROJECT)
    # predictorArn = predictor.create(
    #     datasetGroupArn=dsgArn,
    #     predictorName=predictorName,
    #     region=REGION,forecastHorizon=12
    # )
    # predictor.wait(predictorArn, region=REGION)

    # # ensure that names are different so that new forecast is created        
    # currentTime =  datetime.now().strftime("%Y_%M_%d_%H_%m_%S")
    # forecastName = '{}_cold_forecast_{}'.format(PROJECT, currentTime)
    # forecastArn = forecast.create(region=REGION, forecastName=forecastName, predictorArn=predictorArn)
    # forecast.wait_create(region=REGION, forecastArn=forecastArn)

    exportPath = 's3://{}/forecast/cold.csv'.format(BUCKET_NAME)
    exportJobArn = forecast.export(
        region=REGION,
        forecastArn='arn:aws:forecast:ap-northeast-1:647525257129:forecast/bill_analyzer_cold_forecast_2020_59_13_10_04_45',
        s3exportPath=exportPath,
        exporterRoleArn=EXPORTER_ROLE_ARN,
        jobName='cold_export_job_2'
    )

    wait(what='Exporting data',
        statusFunc=lambda: forecastClnt.describe_forecast_export_job(
            ForecastExportJobArn=exportJobArn,
        )['Status']
    )
    


if __name__ == '__main__':
    main()
    

