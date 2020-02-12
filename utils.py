import datetime
import os
import random
import uuid
import zipfile
from pprint import pprint

import boto3
import botocore.exceptions


DYNAMODB_HOST = 'http://localhost:4569'
LAMBDA_HOST = 'http://localhost:4574'
ELASTICSEARCH_SERVICE_HOST = 'http://localhost:4578'


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

    stream_arn = None
    while True:
        response = dynamodb_client.describe_table(
            TableName='local-user'
        )

        if response['Table']['TableStatus'] != 'ACTIVE':
            continue

        stream_arn = response['Table']['LatestStreamArn']
        break
    return stream_arn


def create_lambda_function():
    lambda_client = boto3.client(
        'lambda',
        endpoint_url=LAMBDA_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo', aws_secret_access_key='bar'
    )

    # compress lambda function
    with zipfile.ZipFile(LAMBDA_HANDLER_SOURCE_FILE_NAME, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('local_lambda_handler/'):
            for file in files:
                zipf.write(os.path.join(root, file), f'{root.replace("local_lambda_handler", "")}/{file}')

    # create lambda function with the compressed file
    with open(LAMBDA_HANDLER_SOURCE_FILE_NAME, mode='rb', ) as fp:
        try:
            lambda_client.create_function(
                FunctionName='user-indexer',
                Runtime='python3.6',
                Code={'ZipFile': fp.read()},
                Role='whatever',
                Handler='handler.lambda_handler'
            )
        except botocore.exceptions.ClientError:
            lambda_client.get_function(
                FunctionName='user-indexer'
            )


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


def create_elasticsearch_domain():
    es_client = boto3.client(
        'es',
        endpoint_url=ELASTICSEARCH_SERVICE_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo', aws_secret_access_key='bar'
    )

    try:
        es_client.create_elasticsearch_domain(
            DomainName="local-es"
        )
    except botocore.exceptions.ClientError as e:
        print(f'ClientError while creating es domain: {str(e)}, possibly the domain has already been made.')


def create_item_on_dynamodb():
    dynamodb_client = boto3.client(
        'dynamodb',
        endpoint_url=DYNAMODB_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo', aws_secret_access_key='bar'
    )

    sample_item = {
        'id': uuid.uuid4().hex,
        'username': f'test_username_{random.randint(0, 1000)}',
        'gender': random.choice(['M', 'F', 'SECRET']),
        'phone_number': f'+82101234{random.randint(0, 9999)}',
        'created_at': datetime.datetime.utcnow().isoformat()
    }

    dynamodb_client.put_item(
        TableName='local-user',
        Item={
            'id': {'S': sample_item['id']},
            'username': {'S': sample_item['username']},
            'gender': {'S': sample_item['gender']},
            'phone_number': {'S': sample_item['phone_number']},
            'created_at': {'S': sample_item['created_at']}
        },
    )

    print()
    print('DynamoDB item has been put with: ')
    pprint(sample_item)
