import boto3
from elasticsearch_dsl import Document, Keyword, Date, connections
from furl import furl

ELASTICSEARCH_SCHEME = 'http'
ELASTICSEARCH_HOST = 'localhost'
ELASTICSEARCH_PORT = '4571'

DYNAMODB_HOST = 'http://localhost:4569'


class UserIndex(Document):
    class Index:
        name = 'user-index'

    username = Keyword()
    gender = Keyword()
    phone_number = Keyword()
    created_at = Date()


def lambda_handler(event, context):
    boto3.resource(
        'dynamodb', endpoint_url=DYNAMODB_HOST,
        region_name='ap-northeast-2',
        aws_access_key_id='foo', aws_secret_access_key='bar'
    )
    deserializer = boto3.dynamodb.types.TypeDeserializer()

    f = furl(
        scheme=ELASTICSEARCH_SCHEME,
        host=ELASTICSEARCH_HOST,
        port=ELASTICSEARCH_PORT
    )

    connections.create_connection(
        hosts=[f.url, ],
    )

    data_list = []
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_image = record['dynamodb']['NewImage']
            data_list.append({
                k: deserializer.deserialize(v)
                for k, v in new_image.items()
            })
        else:
            continue

    ret_val = []
    for item in data_list:
        doc = UserIndex(
            meta={'id': item['id']},
            username=item['username'],
            gender=item.get('gender'),
            phone_number=item['phone_number'],
            created_at=item['created_at']
        )

        doc.save()
        ret_val.append(doc.meta['id'])

    return {
        'doc_ids': ret_val
    }
