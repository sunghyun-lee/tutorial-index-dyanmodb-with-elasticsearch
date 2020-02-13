import sys

import boto3

DYNAMODB_HOST = 'http://localhost:4569'


def scan_items():
    number_of_items = 0
    dynamodb_client = boto3.client(
        'dynamodb',
        endpoint_url=DYNAMODB_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo',
        aws_secret_access_key='bar'
    )

    response = dynamodb_client.scan(
        TableName='local-user',
        Limit=10
    )
    exclusive_start_key = response['LastEvaluatedKey']
    number_of_items += response['Count']

    while True:
        response = dynamodb_client.scan(
            TableName='local-user',
            Limit=10,
            ExclusiveStartKey=exclusive_start_key
        )

        number_of_items += response['Count']
        try:
            exclusive_start_key = response['LastEvaluatedKey']
        except KeyError:
            break
        sys.stdout.write("\r")
        sys.stdout.write(f'{number_of_items} items scanned.')
        sys.stdout.flush()

    print(f'\n\nfinished: TotalCount of Items: {number_of_items}')

    return


if __name__ == "__main__":
    scan_items()
