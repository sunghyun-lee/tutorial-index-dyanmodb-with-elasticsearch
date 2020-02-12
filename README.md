# Index DynamoDB data into ElasticSearch on local machine

This is the code for demonstration purpose of my presentation, `How To Use Elasticsearch Dynamodb`.

### Prerequisite

- docker (https://docs.docker.com/docker-for-mac/install/)
- poetry (https://python-poetry.org/docs/#installation)

After installing these two things, run the command:

> $ poetry install

This will install of python packages which are necessary to run this snippet.

### Deploy localstack

1. Run 

> $ docker-compose up -d localstack

2. After docker has been deployed, you can check if the

 