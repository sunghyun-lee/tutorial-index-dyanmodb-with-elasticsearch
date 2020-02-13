import datetime
import string
import uuid
import random
import boto3

DYNAMODB_HOST = 'http://localhost:4569'


def put_item():
    dynamodb_client = boto3.client(
        'dynamodb',
        endpoint_url=DYNAMODB_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo',
        aws_secret_access_key='bar'
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
            'created_at': {'S': sample_item['created_at']},
            'password': {'S': uuid.uuid4().hex},
            'note': {'S': 'Hello, I am {username}, and I am {random_string} as well.'.format(
                username=sample_item['username'],
                random_string=''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(8))
            )}
        },
    )


def batch_write_items(number_of_loop=20):
    dynamodb_client = boto3.client(
        'dynamodb',
        endpoint_url=DYNAMODB_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo',
        aws_secret_access_key='bar'
    )

    for _ in range(number_of_loop):
        sample_items = [{
            'id': uuid.uuid4().hex,
            'username': f'test_username_{random.randint(0, 1000)}',
            'gender': random.choice(['M', 'F', 'SECRET']),
            'phone_number': f'+82101234{random.randint(0, 9999)}',
            'created_at': datetime.datetime.utcnow().isoformat()
        } for _ in range(25)]

        dynamodb_client.batch_write_item(
            RequestItems={
                'local-user': [{
                    'PutRequest': {
                        'Item': {
                            'id': {'S': sample_item['id']},
                            'username': {'S': sample_item['username']},
                            'gender': {'S': sample_item['gender']},
                            'phone_number': {'S': sample_item['phone_number']},
                            'created_at': {'S': sample_item['created_at']},
                            'password': {'S': uuid.uuid4().hex},
                            'note': {'S': 'Hello, I am {username}, and I am {random_string} as well.'.format(
                                username=sample_item['username'],
                                random_string=''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(8))
                            )}
                        },
                    }
                } for sample_item in sample_items]
            }
        )


if __name__ == "__main__":
    put_item()
    batch_write_items(20)
