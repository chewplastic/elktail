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


def get_search_body(iso_date, service_name=None, service_type=None, max_date=None, message=None, search_after=None, important=False):
    timestamp_range = {"gt": f"{iso_date}Z"}
    if max_date is not None:
        timestamp_range["lte"] = f"{max_date}Z"

    body = {
        "size": 500,
        "sort": [
            {"@timestamp": {"order": "asc"}},
            "_doc"
        ],
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

    # Add search_after for pagination
    if search_after is not None:
        body['search_after'] = search_after

    if service_name is not None:
        body['query']['bool']['must'].append(
            {
                'match': {
                    'service.name': service_name
                }
            }
        )
    if service_type is not None:
        body['query']['bool']['must'].append(
            {
                'match': {
                    'service.type': service_type
                }
            }
        )
    if message is not None:
        body['query']['bool']['must'].append(
            {
                'wildcard': {
                    'message': message
                }
            }
        )
    if important:
        body['query']['bool']['must'].append(
            {
                'terms': {
                    'log.level': ['error', 'warning']
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

    return es.search(
        body=body,
        index=index_pattern
    )


def search_all_pages(es, iso_date, service_name=None, service_type=None, max_date=None, message=None, index_pattern=None, important=False):
    """
    Generator that yields documents page by page using search_after pagination.
    Yields each document as it's fetched, allowing for streaming output.
    """
    if index_pattern is None:
        config = configuration.get_config()
        index_pattern = config['index_pattern']

    search_after = None
    page_num = 0
    total_docs = 0

    # ANSI color codes
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RESET = '\033[0m'
    SAVE_CURSOR = '\033[s'
    RESTORE_CURSOR = '\033[u'
    CLEAR_LINE = '\033[K'

    while True:
        body = get_search_body(iso_date, service_name, service_type, max_date, message, search_after, important)

        # # output the body for debugging (only first page to avoid spam)
        # if page_num == 0:
        #     print(f"\n{CYAN}=== Elasticsearch Query ==={RESET}")
        #     print(f"Index: {YELLOW}{index_pattern}{RESET}")
        #     print(f"Body:\n{json.dumps(body, indent=2)}")
        #     print(f"{CYAN}==========================={RESET}\n")

        response = search(es, body, index_pattern)

        hits = response['hits']['hits']
        if not hits:
            break

        # Yield each document as we get it
        for hit in hits:
            yield hit

        page_num += 1
        total_docs += len(hits)

        # Status is now printed in get_lines() where we can track unique vs duplicate docs

        # Get the sort values from the last document for the next page
        search_after = hits[-1]['sort']

        # If we got fewer results than the page size, we're done
        if len(hits) < body['size']:
            break

