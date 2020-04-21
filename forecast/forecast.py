import os
import boto3
from datetime import datetime

from .workflow import ds_group, forecast_role, upload_dataset, predictor, forecast
from .workflow.util import wait


# take data and train model
def do_forecast(region, bucketName, project, trainData):
    currentTime =  datetime.now().strftime("%Y_%M_%d_%H_%m_%S")

    predictorName='{}_deep_arp_algo'.format(project)
    forecastName = '{}_forecast_{}'.format(project, currentTime)
    importJobName = "TRAIN_DATA_IMPORT_" + currentTime

    # create data set for projecct
    dsgArn, dsArn = ds_group.create(
        region=region, bucketName=bucketName, project=project   
    )
    roleArn = forecast_role.create(wait=5)
    

    # =============== import the data ===============
    importJobArn = upload_dataset.upload(
        datasetImportJobName=importJobName,
        datasetGroupArn=dsgArn,
        datasetArn=dsArn,
        roleArn=roleArn,
        bucketName=bucketName,
        # filePath="./item-demand-time-train.csv",
        filePath=trainData,
        fileKey="bill-analyzer-data/train.csv",
    )
    upload_dataset.wait(importJobArn, region=region)


    # =============== create the predictor ===============
    # if predictor name is duplicate, then it's not created
    predictorArn = predictor.create(
        datasetGroupArn=dsgArn,
        predictorName=predictorName,
        region=region,forecastHorizon=1 # predict only 1
    )
    predictor.wait(predictorArn, region=region)
    predictorArn = 'arn:aws:forecast:ap-northeast-1:647525257129:predictor/bill_analyzer_deep_arp_algo'
    # =============== create forecast and export  ===============
    # ensure that names are different so that new forecast is created        

    
    forecastArn = forecast.create(region=region, forecastName=forecastName, predictorArn=predictorArn)
    forecast.wait_create(region=region, forecastArn=forecastArn)

    return forecastArn

def query(region, forecastArn, item_id):
    return forecast.query(region=region, forecastArn=forecastArn, filters={"item_id": item_id})['Forecast']['Predictions']


import dotenv
if __name__ == "__main__":
    dotenv.load_dotenv()
    REGION = os.getenv('REGION')
    print(query(
        REGION,
        'arn:aws:forecast:ap-northeast-1:647525257129:forecast/bill_analyzer_forecast_2020_52_14_12_04_42',
        'u8b7885ba3814bbc86a45cd1a0a82e6ce',
    ))
