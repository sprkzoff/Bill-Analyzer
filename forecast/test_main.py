import dotenv
from workflow import ds_group, forecast_role, upload_dataset, predictor
import os

dotenv.load_dotenv()


def main():
    REGION = os.getenv('REGION')
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    PROJECT = os.getenv('PROJECT')

    dsgArn, dsArn = ds_group.create(
        region=REGION, bucketName=BUCKET_NAME, project=PROJECT   
    )
    roleArn = forecast_role.create(wait=5)
    # importJobArn = upload_dataset.upload(
    #     datasetGroupArn=dsgArn,
    #     datasetArn=dsArn,
    #     roleArn=roleArn,
    #     bucketName=BUCKET_NAME,
    #     filePath="./item-demand-time-train.csv",
    #     fileKey="bill-analyzer-data/train.csv",
    # )
    # upload_dataset.wait(importJobArn, region=REGION)

    
    predictorArn = predictor.create(datasetGroupArn=dsgArn, project=PROJECT, region=REGION, forecastHorizon=12)
    predictor.wait(predictorArn, region=REGION)
    


if __name__ == '__main__':
    main()
    

