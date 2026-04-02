import json
import boto3
import time

step_client = boto3.client('stepfunctions')

STATE_MACHINE_ARN = "arn:aws:states:us-east-1:740811899782:stateMachine:learning-data-engineering"


def is_pipeline_running():
    response = step_client.list_executions(
        stateMachineArn=STATE_MACHINE_ARN,
        statusFilter='RUNNING'
    )
    return len(response['executions']) > 0


def lambda_handler(event, context):
    print("Lambda triggered")
    print(event)
    file_keys = []

    # Extract S3 keys from SQS messages
    for record in event['Records']:
        body = json.loads(record['body'])
        s3_event = body['Records'][0]
        key = s3_event['s3']['object']['key']
        file_keys.append(key)

    # Remove duplicates
    file_keys = list(set(file_keys))

    print("Files received:", file_keys)

    # Check if pipeline already running
    if is_pipeline_running():
        print("Pipeline already running. Skipping trigger.")
        return {"statusCode": 200}

    # Trigger Step Function once
    step_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=str(int(time.time())),  # unique execution name
        input=json.dumps({
            "files": file_keys
        })
    )

    return {"statusCode": 200}