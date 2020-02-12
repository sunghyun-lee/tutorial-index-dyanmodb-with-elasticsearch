import boto3
import botocore.exceptions


DYNAMODB_HOST = 'http://localhost:4569'
LAMBDA_HOST = 'http://localhost:4574'

LAMBDA_HANDLER_SOURCE_FILE_NAME = 'lambda_f.zip'


def prepare_dynamodb_table():
    dynamodb_client = boto3.client(
        'dynamodb',
        endpoint_url=DYNAMODB_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo', aws_secret_access_key='bar'
    )

    dynamodb_client.create_table(
        TableName='local-user',
        AttributeDefinitions=[{
            'AttributeName': 'id',
            'AttributeType': 'S'
        }],
        KeySchema=[{
            'AttributeName': 'id',
            'KeyType': 'HASH'
        }],
        BillingMode='PAY_PER_REQUEST',
        StreamSpecification={
            'StreamEnabled': True,
            'StreamViewType': 'NEW_IMAGE'
        },
    )

    print('waiting to finish creation of table')

    stream_arn = None
    while True:
        response = dynamodb_client.describe_table(
            TableName='local-user'
        )

        if response['Table']['TableStatus'] != 'ACTIVE':
            continue

        stream_arn = response['Table']['LatestStreamArn']
        break
    print('table has been created')

    return stream_arn


def create_lambda_function():
    lambda_client = boto3.client(
        'lambda',
        endpoint_url=LAMBDA_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo', aws_secret_access_key='bar'
    )

    try:
        response = lambda_client.create_function(
            FunctionName='user-indexer',
            Runtime='python3.6',
            Code={'S3Bucket': 'local-lambda-function-bucket', 'S3Key': LAMBDA_HANDLER_SOURCE_FILE_NAME},
            Role='role',
            Handler='handler.lambda_handler'
        )
    except botocore.exceptions.ClientError as e:
        response = lambda_client.get_function(
            FunctionName='user-indexer'
        )

        return response['Configuration']

    return response['FunctionArn']


def create_event_source_mapping(stream_arn):
    lambda_client = boto3.client(
        'lambda',
        endpoint_url=LAMBDA_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo', aws_secret_access_key='bar'
    )

    lambda_client.create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName='user-indexer',
        Enabled=True,
    )
