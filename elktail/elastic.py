import configparser
import json
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


def get_search_body(iso_date, service_name=None, service_type=None, max_date=None, message=None):
    timestamp_range = {"gt": f"{iso_date}Z"}
    if max_date is not None:
        timestamp_range["lte"] = f"{max_date}Z"

    body = {
        "source": {
            "size": 1000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": timestamp_range
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
    if message is not None:
        body['source']['query']['bool']['must'].append(
            {
                'wildcard': {
                    'message': message
                }
            }
        )
    return body


def search(es, body, index_pattern=None):
    if index_pattern is None:
        config = configuration.get_config()
        index_pattern = config['index_pattern']

    # print(f"\n=== Elasticsearch Query ===")
    # print(f"Index: {index_pattern}")
    # print(f"Body:\n{json.dumps(body, indent=2)}")
    # print("===========================\n")

    return es.search_template(
        body,
        index=index_pattern
    )

