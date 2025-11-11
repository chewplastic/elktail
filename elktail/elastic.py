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


def get_search_body(iso_date, project=None, process_type=None,
                    environment=None):
    body = {
        "source": {
            "size": 1000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gt": f"{iso_date}Z"
                                }
                            }
                        }
                    ]
                }
            }
        }
    }
    if project is not None:
        body['source']['query']['bool']['must'].append(
            {
                'match': {
                    'fields.project.keyword': project
                }
            }
        )
    if process_type is not None:
        body['source']['query']['bool']['must'].append(
            {
                'match': {
                    'fields.process_type.keyword': process_type
                }
            }
        )
    if environment is not None:
        body['source']['query']['bool']['must'].append(
            {
                'match': {
                    'fields.environment.keyword': environment
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
        index=f"filebeat-{now.year}.{now.month:02}.{now.day:02}"
    )

