import configparser
from elasticsearch import Elasticsearch
from datetime import datetime

from elktail import configuration


def connect():
    config = configuration.get_config()
    return Elasticsearch(
        [config['host']],
        http_auth=(config['username'], config['password']),
        scheme=config['scheme'], port=config['port']
    )


def get_search_body(iso_date, service_name=None, service_type=None):
    body = {
        "source": {
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": f"{iso_date}Z"
                                }
                            }
                        }
                    ]
                }
            }
        }
    }
    if service_name is not None:
        body['source']['query']['bool']['must'].append(
            {
                'match': {
                    'service.name': service_name
                }
            }
        )
    if service_type is not None:
        body['source']['query']['bool']['must'].append(
            {
                'match': {
                    'service.type': service_type
                }
            }
        )
    return body


def search(es, body, index_pattern=None):
    if index_pattern is None:
        config = configuration.get_config()
        index_pattern = config['index_pattern']
    return es.search_template(
        body,
        index=index_pattern
    )

