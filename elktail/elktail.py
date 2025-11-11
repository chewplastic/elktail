#!/usr/bin/env python

import sys
import time
from datetime import datetime, timedelta, timezone
from optparse import OptionParser
import json
import re

from elktail import elastic


def parse_timestamp(timestamp_str):
    """
    Parse timestamp string in various ISO 8601 formats.
    Handles:
    - UTC with Z: 2025-11-11T14:01:41.977Z
    - With timezone offset: 2025-11-11T14:01:41.977-08:00
    - Without microseconds: 2025-11-11T14:01:41Z
    """
    # List of formats to try
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",           # 2025-11-11T14:01:41.977Z
        "%Y-%m-%dT%H:%M:%SZ",               # 2025-11-11T14:01:41Z
        "%Y-%m-%dT%H:%M:%S.%f%z",          # 2025-11-11T14:01:41.977-08:00
        "%Y-%m-%dT%H:%M:%S%z",             # 2025-11-11T14:01:41-08:00
    ]

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue

    # If none of the formats work, try a more flexible approach
    # Remove timezone and parse
    try:
        # Remove timezone offset like +00:00 or -08:00
        ts_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', timestamp_str)
        # Remove Z suffix
        ts_clean = ts_clean.rstrip('Z')

        if '.' in ts_clean:
            return datetime.strptime(ts_clean, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            return datetime.strptime(ts_clean, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise ValueError(f"Unable to parse timestamp: {timestamp_str}")


def get_lines(client, iso_date, service_name, service_type, index_pattern=None):
    body = elastic.get_search_body(iso_date, service_name, service_type)
    response = elastic.search(client, body, index_pattern)
    new_ts = None
    lines = list()
    for doc in response['hits']['hits']:
        ts = doc['_source']['@timestamp']
        if "message" in doc['_source']:
            message = doc['_source']['message']
            lines.append(f"{ts} :: {message}")
        new_ts = parse_timestamp(doc['_source']['@timestamp']) + timedelta(milliseconds=400)
        new_ts = new_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")

        # Track the maximum timestamp
        current_ts = parse_timestamp(doc['_source']['@timestamp'])
        if max_ts is None or current_ts > max_ts:
            max_ts = current_ts

    # Format max timestamp
    if max_ts is not None:
        max_ts = max_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")

    return max_ts, lines


def show_lines(lines):
    for line in lines:
        print(line)


def mainloop(service_name=None, service_type=None, index_pattern=None):
    client = elastic.connect()
    iso_date = datetime.now(timezone.utc).isoformat()
    last = None
    while True:
        iso_date, lines = get_lines(
            client,
            iso_date,
            service_name,
            service_type,
            index_pattern
        )
        show_lines(lines)

        if iso_date is None:
            if last is None:
               last = datetime.now(timezone.utc).isoformat()
            iso_date = last
        else:
            last = iso_date

        time.sleep(2)


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("-n", "--service-name", dest="service_name",
        help="[optional] filter by service name (queries service.name field)")
    parser.add_option("-t", "--service-type", dest="service_type",
        help="[optional] filter by service type (queries service.type field)")
    parser.add_option("-i", "--index", dest="index_pattern",
        help="[optional] index pattern to query (e.g., logstash-*, my-logs-*)")
    (options, args) = parser.parse_args()

    mainloop(
       service_name=options.service_name,
       service_type=options.service_type,
       index_pattern=options.index_pattern
    )
