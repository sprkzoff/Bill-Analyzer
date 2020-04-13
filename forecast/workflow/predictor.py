import boto3
import os
import time
from botocore.exceptions import ClientError
from .util import extract_arn_from_error

getFeatureConfig = lambda freq: {
    "ForecastFrequency": freq,
    "Featurizations": [
        {
            "AttributeName": "target_value",
            "FeaturizationPipeline": [
                {
                    "FeaturizationMethodName": "filling",
                    "FeaturizationMethodParameters":
                    {
                        "frontfill": "none",
                        "middlefill": "zero",
                        "backfill": "zero"
                    }
                }
            ]
        }
    ]
}

def create(
    datasetGroupArn,
    predictorName,
    region,
    forecastHorizon=24,
    algorithmArn='arn:aws:forecast:::algorithm/Deep_AR_Plus'
):
    print("="*10, "Creating Predictor with {} algorithm".format(algorithmArn), "="*10)
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name="forecast")

    FORECAST_FREQUENCY = os.getenv('DATASET_FREQUENCY') # same as freq
    predictorName = project+'_deeparp_algo'
    try:
        create_predictor_response = forecast.create_predictor(
            PredictorName=predictorName,
            AlgorithmArn=algorithmArn,
            ForecastHorizon=forecastHorizon,
            PerformAutoML=False,
            PerformHPO=False,
            EvaluationParameters={
                "NumberOfBacktestWindows": 1,
                "BackTestWindowOffset": 24
            },
            InputDataConfig={"DatasetGroupArn": datasetGroupArn},
            FeaturizationConfig=getFeatureConfig(FORECAST_FREQUENCY)
        )
        predictorArn = create_predictor_response['PredictorArn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            predictorArn = extract_arn_from_error(e)
            print("Predictor ARN: {} already exists, ignoring".format(predictorArn))
        else:
            print("Unexpected error:", e)
            raise e
    return predictorArn


def wait(predictorArn, region):
    print("="*10, "Waiting for predictor to be trained", "="*10)
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name='forecast')

    lastStatus = None
    while True:
        status = forecast.describe_predictor(
            PredictorArn=predictorArn,
        )['Status']
        if status != lastStatus:
            print("\n" + status, end="")
            lastStatus = status
        else:
            print(".", end="")

        if status == 'ACTIVE' or status == 'FAILED':
            break
        time.sleep(10)

    print('\nResult:', status)
