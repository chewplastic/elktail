# ELKTAIL

## Description

ELKTAIL is a tool that generates a tail-like stream from Elasticsearch logs.

The tool works with any Elasticsearch log index and uses the Elastic Common Schema (ECS) fields:

* service.name
* service.type

You can configure which index pattern to query (e.g., `log-*`, `log-swarm-*`, `filebeat-*`, etc.).

## Installation

### Docker (Recommended)

The easiest way to run elktail is using Docker:

```bash
# Clone the repository
git clone git@github.com:chewplastic/elktail.git
cd elktail/

# Create config directory
mkdir -p config

# Run with filters (eg: auth.webservice in log-* index containing a message field with "*sql*")
docker compose run --rm elktail -n "auth" -t "webservice" -i "log-*" -m "sql"
```

The configuration will be saved in `./config/config.ini` and persisted across container restarts.

## Configuration

The first time this tool gets executed (or if the configuration file is
missing) the initial configuration process kicks in. The following
parameters will be requested:

* host: url or ip of the elasticsearch server
* index pattern: pattern to match log indices (default: `log-*`)

## Usage

```
$ docker compose run --rm elktail --help
```

* Arguments can be used at the same time or no arguments at all
* Arguments works as **and** filters

### No arguments

Executing elktail with no arguments will **tail** everything from the configured
index pattern.
