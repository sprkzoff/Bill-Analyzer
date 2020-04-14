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

    # config here to determine whether create new resource or not
    # duplicate will not be created
    currentTime =  datetime.now().strftime("%Y_%M_%d_%H_%m_%S")

    predictorName='{}_deep_arp_algo'.format(PROJECT)
    forecastName = '{}_forecast_{}'.format(PROJECT, currentTime)
    importJobName = "TRAIN_DATA_IMPORT_" + currentTime
    coldImportJobName = "COLD_DATA_IMPORT_" + currentTime

    # shouldImportTrain = False
    # shouldCreatePredictor = False
    # shouldImportCold = True
    # shouldForecast = True
    # shouldExport = False

    # clients
    session = boto3.Session(region_name=REGION)
    forecastClnt = session.client(service_name='forecast')

    # create data set for projecct
    dsgArn, dsArn = ds_group.create(
        region=REGION, bucketName=BUCKET_NAME, project=PROJECT   
    )
    roleArn = forecast_role.create(wait=5)
    
    

    # =============== import the data ===============
    # importJobArn = upload_dataset.upload(
    #     datasetImportJobName=importJobName,
    #     datasetGroupArn=dsgArn,
    #     datasetArn=dsArn,
    #     roleArn=roleArn,
    #     bucketName=BUCKET_NAME,
    #     # filePath="./item-demand-time-train.csv",
    #     filePath="./train_data.csv",
    #     fileKey="bill-analyzer-data/train.csv",
    # )
    # upload_dataset.wait(importJobArn, region=REGION)


    # =============== create the predictor ===============
    # if predictor name is duplicate, then it's not created
    # predictorArn = predictor.create(
    #     datasetGroupArn=dsgArn,
    #     predictorName=predictorName,
    #     region=REGION,forecastHorizon=12
    # )
    # predictor.wait(predictorArn, region=REGION)
    predictorArn = 'arn:aws:forecast:ap-northeast-1:647525257129:predictor/bill_analyzer_deep_arp_algo'
    # =============== overwrite old data ==================
    

    coldImportJobArn = upload_dataset.upload(
        datasetImportJobName=coldImportJobName,
        datasetGroupArn=dsgArn,
        datasetArn=dsArn,
        roleArn=roleArn,
        filePath="./cold_data.csv",
        bucketName=BUCKET_NAME,
        fileKey="bill-analyzer-data/cold.csv"
    )
    upload_dataset.wait(coldImportJobArn, region=REGION)



    # =============== create forecast and export  ===============
    # ensure that names are different so that new forecast is created        

    forecastArn = forecast.create(region=REGION, forecastName=forecastName, predictorArn=predictorArn)
    forecast.wait_create(region=REGION, forecastArn=forecastArn)

    exportPath = 's3://{}/forecast/train.csv'.format(BUCKET_NAME)
    exportJobArn = forecast.export(
        region=REGION,
        forecastArn=forecastArn,
        s3exportPath=exportPath,
        exporterRoleArn=EXPORTER_ROLE_ARN,
        jobName='train_export_job_' + currentTime
    )

    wait(what='Exporting data',
        statusFunc=lambda: forecastClnt.describe_forecast_export_job(
            ForecastExportJobArn=exportJobArn,
        )['Status']
    )

    

if __name__ == '__main__':
    main()
    

