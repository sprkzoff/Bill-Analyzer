import boto3
import os
import time
from botocore.exceptions import ClientError
from .util import extract_arn_from_error, wait as wait_util

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
    datasetGroupArn=None,
    # specify project name (will be appended by _deep_arp_algo) OR manually predictor name
    predictorName=None,
    project=None,
    region=None,
    forecastHorizon=1,
    algorithmArn='arn:aws:forecast:::algorithm/ETS'
):
    print("="*10, "Creating Predictor with {} algorithm".format(algorithmArn), "="*10)
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name="forecast")

    FORECAST_FREQUENCY = os.getenv('DATASET_FREQUENCY') # same as freq
    if predictorName is None and project is not None:
        predictorName = project + "_deeparp_model"
        
    try:
        create_predictor_response = forecast.create_predictor(
            PredictorName=predictorName,
            AlgorithmArn=algorithmArn,
            ForecastHorizon=forecastHorizon,
            PerformAutoML=False,
            PerformHPO=False,
            EvaluationParameters={
                "NumberOfBacktestWindows": 1,
                "BackTestWindowOffset": forecastHorizon,
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
    session = boto3.Session(region_name=region)
    forecast = session.client(service_name='forecast')

    wait_util(
        what="predictor to be trained",
        statusFunc = lambda: forecast.describe_predictor(
            PredictorArn=predictorArn,
        )['Status'],
    )