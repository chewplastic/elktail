#!/usr/bin/env python

import sys
import time
from datetime import datetime, timedelta
from optparse import OptionParser
import json

from elktail import elastic


def get_lines(client, iso_date, project, process_type, environment, index_pattern=None):
    body = elastic.get_search_body(iso_date, project, process_type, environment)
    response = elastic.search(client, body, index_pattern)
    new_ts = None
    lines = list()
    for doc in response['hits']['hits']:
        ts = doc['_source']['@timestamp']
        if "message" in doc['_source']:
            message = doc['_source']['message']
            lines.append(f"{ts} :: {message}")
        new_ts = datetime.strptime(
                     doc['_source']['@timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ"
                 ) + timedelta(milliseconds=400)
        new_ts = new_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")

    return new_ts, lines


def show_lines(lines):
    for line in lines:
        print(line)


def mainloop(project=None, process_type=None, environment=None, index_pattern=None):
    client = elastic.connect()
    iso_date = datetime.utcnow().isoformat()
    last = None
    while True:
        iso_date, lines = get_lines(
            client,
            iso_date,
            project,
            process_type,
            environment,
            index_pattern
        )
        show_lines(lines)

        if iso_date is None:
            if last is None:
               last = datetime.utcnow().isoformat()
            iso_date = last
        else:
            last = iso_date

        time.sleep(2)


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("-p", "--project", dest="project",
        help="[optional] select the project that logs will be displayed")
    parser.add_option("-t", "--process_type", dest="process_type",
        help="[optional] select the process type that logs will be displayed")
    parser.add_option("-e", "--environment", dest="environment",
        help="[optional] environment")
    parser.add_option("-i", "--index", dest="index_pattern",
        help="[optional] index pattern to query (e.g., logstash-*, my-logs-*)")
    (options, args) = parser.parse_args()

    mainloop(
       project=options.project,
       process_type=options.process_type,
       environment=options.environment,
       index_pattern=options.index_pattern
    )
