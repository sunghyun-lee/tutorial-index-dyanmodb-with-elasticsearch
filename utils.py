import boto3

DYNAMODB_HOST = 'http://localhost:4569'


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
