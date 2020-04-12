# used for extracting ARN from already exists error message
def extract_arn_from_error(error):
    msg = error.response['Error']['Message']
    idx = msg.find("arn: ") + 5
    datasetGroupArn = msg[idx:]

    return datasetGroupArn