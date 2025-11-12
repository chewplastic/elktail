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


def get_lines(client, iso_date, service_name, service_type, index_pattern=None, message=None, seen_ids=None, last_search_after=None, quiet=False, important=False):
    # Always search ending at current time
    current_time = datetime.now(timezone.utc)

    # If we have a last position, start from 30 seconds before it to catch late arrivals
    # Otherwise, search the full 2-minute window
    if iso_date:
        min_date = iso_date
    else:
        min_date = (current_time - timedelta(minutes=2)).isoformat()

    max_date = current_time.isoformat()

    max_ts = None
    lines = list()
    final_search_after = None
    docs_fetched = 0
    docs_printed = 0
    page_docs_fetched = 0
    page_docs_printed = 0
    page_num = 0

    # ANSI color codes
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    RESET = '\033[0m'

    # Clean up old IDs (older than 5 minutes)
    if seen_ids is not None:
        cutoff_time = current_time - timedelta(minutes=5)
        ids_to_remove = [doc_id for doc_id, seen_time in seen_ids.items() if seen_time < cutoff_time]
        for doc_id in ids_to_remove:
            del seen_ids[doc_id]

    # Use search_all_pages generator to get results page by page
    # Search the last 5 minutes ending at current time
    for doc in elastic.search_all_pages(client, min_date, service_name, service_type, max_date, message, index_pattern, important):
        docs_fetched += 1
        page_docs_fetched += 1
        doc_id = doc['_id']

        # Check if this is a duplicate
        is_duplicate = seen_ids is not None and doc_id in seen_ids

        if not is_duplicate:
            # Mark this document as seen
            if seen_ids is not None:
                seen_ids[doc_id] = current_time

            ts = doc['_source']['@timestamp']
            if "message" in doc['_source']:
                message_text = doc['_source']['message']
                service_type_val = doc['_source'].get('service', {}).get('type', 'unknown')
                service_name_val = doc['_source'].get('service', {}).get('name', 'unknown')
                # get log.level if exists and make it have a fixed width
                log_level = doc['_source'].get('log', {}).get('level', '?').upper().ljust(7)

                # Format line with service name, type, and log level
                line = f"[{log_level}] {ts} :: [{service_name_val}.{service_type_val}] :: {message_text}"
            else:
                line = f"{ts} :: {json.dumps(doc['_source'])}"

            # Output the line immediately
            print(line, flush=True)
            docs_printed += 1
            page_docs_printed += 1

            # Track the maximum timestamp
            current_ts = parse_timestamp(doc['_source']['@timestamp'])
            if max_ts is None or current_ts > max_ts:
                max_ts = current_ts

        # Print page status every 500 documents (page size) - regardless of duplicates
        if page_docs_fetched % 500 == 0:
            page_num += 1
            if not quiet:
                skipped = page_docs_fetched - page_docs_printed
                print(f"{GREEN}[Page {page_num}]{RESET} {YELLOW}{page_docs_fetched}{RESET} docs fetched, "
                      f"{MAGENTA}{page_docs_printed}{RESET} unique printed "
                      f"{CYAN}({skipped} duplicates){RESET}", flush=True)
            # Reset page counters
            page_docs_fetched = 0
            page_docs_printed = 0

    # Print final page status if there's a partial page
    if page_docs_fetched > 0 and not quiet:
        page_num += 1
        skipped = page_docs_fetched - page_docs_printed
        print(f"{GREEN}[Page {page_num}]{RESET} {YELLOW}{page_docs_fetched}{RESET} docs fetched, "
              f"{MAGENTA}{page_docs_printed}{RESET} unique printed "
              f"{CYAN}({skipped} duplicates){RESET}", flush=True)

    # # Print final summary
    # if docs_fetched > 0:
    #     total_skipped = docs_fetched - docs_printed
    #     print(f"\n{CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    #     print(f"{GREEN}Query complete:{RESET} {YELLOW}{docs_fetched}{RESET} total documents, "
    #           f"{MAGENTA}{docs_printed}{RESET} unique printed, "
    #           f"{CYAN}{total_skipped} duplicates skipped{RESET}")
    #     print(f"{CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}\n", flush=True)

    # Calculate next starting point: go back 30 seconds from max to catch late arrivals
    next_start = None
    if max_ts is not None:
        next_start = (max_ts - timedelta(seconds=30)).isoformat()

    return next_start, []  # Return next starting point and empty lines since we're printing as we go


def show_lines(lines):
    for line in lines:
        print(line)


def mainloop(service_name=None, service_type=None, index_pattern=None, message=None, quiet=False, important=False):
    client = elastic.connect()
    seen_ids = {}  # Track document IDs to prevent duplicates
    last_timestamp = None  # Track where we left off

    while True:
        # Query from last position (or full window on first run)
        last_timestamp, lines = get_lines(
            client,
            last_timestamp,  # Start from last position (or None for full window)
            service_name,
            service_type,
            index_pattern,
            message,
            seen_ids,
            None,  # last_search_after
            quiet,
            important
        )
        show_lines(lines)

        time.sleep(1)


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("-n", "--service-name", dest="service_name",
        help="[optional] filter by service name (queries service.name field)")
    parser.add_option("-t", "--service-type", dest="service_type",
        help="[optional] filter by service type (queries service.type field)")
    parser.add_option("-i", "--index", dest="index_pattern",
        help="[optional] index pattern to query (e.g., logstash-*, my-logs-*)")
    parser.add_option("-m", "--message", dest="message",
        help="[optional] filter by message content with wildcards (e.g., '*error*', '*auth*failed*')")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
        help="[optional] disable pagination output")
    parser.add_option("--important", action="store_true", dest="important",
        help="[optional] only show error and warning level logs")
    (options, args) = parser.parse_args()

    mainloop(
       service_name=options.service_name,
       service_type=options.service_type,
       index_pattern=options.index_pattern,
       message=options.message,
       quiet=options.quiet,
       important=options.important
    )
