import time


# used for extracting ARN from already exists error message
def extract_arn_from_error(error):
    msg = error.response['Error']['Message']
    idx = msg.find("arn: ") + 5
    datasetGroupArn = msg[idx:]

    return datasetGroupArn

def wait(statusFunc, what):
    print("="*10, "Waiting for {}".format(what), "="*10)

    lastStatus = None
    while True:
        status = statusFunc()
        if status != lastStatus:
            print("\n" + status, end="")
            lastStatus = status
        else:
            print(".", end="")

        if status == 'ACTIVE' or status == 'FAILED':
            break
        time.sleep(10)

    print('\nResult:', status)