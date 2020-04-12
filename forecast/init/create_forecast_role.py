import boto3
import json
import time

# note: this require  ADMIN ACCESS
def create_forecast_role():
    iam = boto3.client("iam")

    role_name = "ForecastRoleDemo"
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
              "Effect": "Allow",
              "Principal": {
                "Service": "forecast.amazonaws.com"
              },
              "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        create_role_response = iam.create_role(
            RoleName = role_name,
            AssumeRolePolicyDocument = json.dumps(assume_role_policy_document)
        )
        role_arn = create_role_response["Role"]["Arn"]
    except iam.exceptions.EntityAlreadyExistsException:
        print("The role " + role_name + " exists, ignore to create it")
        role_arn = boto3.resource('iam').Role(role_name).arn
        
    # Attaching AmazonForecastFullAccess to access all actions for Amazon Forecast
    policy_arn = "arn:aws:iam::aws:policy/AmazonForecastFullAccess"
    iam.attach_role_policy(
        RoleName = role_name,
        PolicyArn = policy_arn
    )

    # Now add S3 support
    iam.attach_role_policy(
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess',
        RoleName=role_name
    )
    time.sleep(60) # wait for a minute to allow IAM role policy attachment to propagate

    return role_arn