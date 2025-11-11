import os
import sys
import errno
import configparser


def get_config():
    config_path = os.path.join(
        os.environ.get("HOME"),
        ".config/elktail/config.ini"
    )

    if not os.path.exists(config_path):
        print("your elktail is not configured")
        opt = str(input("would you like to configure it now? <Y/n>: "))

        if opt in ['Y', 'y', 'yes', 'YES']:
            config_creator(config_path)

        sys.exit(-1)

    config = configparser.ConfigParser()
    config.read(config_path)
    return {
        'host': config['default']['host'],
        'username': '',
        'scheme': 'http',
        'password': '',
        'port': 9200,
        'index_pattern': config['default'].get('index_pattern', 'log-*')
    }


def config_creator(config_path):
    print(f"creating configuration file: {config_path}")

    try:
        os.makedirs(
            os.path.join(
                os.environ.get("HOME"),
                ".config/elktail"
            )
        )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    host = input("elasticsearch host: ")
    index_pattern = input("index pattern (default: log-*): ") or "log-*"

    config = configparser.RawConfigParser()
    config.add_section("default")
    config.set("default", "host", host)
    config.set("default", "index_pattern", index_pattern)

    with open(config_path, 'w') as configfile:
        config.write(configfile)

    print("elktail configured")
