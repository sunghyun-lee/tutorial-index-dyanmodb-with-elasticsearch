import utils


if __name__ == "__main__":
    stream_arn = utils.prepare_dynamodb_table()
    utils.create_lambda_function()
    utils.create_elasticsearch_domain()
    utils.create_event_source_mapping(stream_arn)
    utils.create_item_on_dynamodb()
