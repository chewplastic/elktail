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
        'username': config['default']['username'],
        'scheme': config['default']['scheme'],
        'password': config['default']['password'],
        'port': int(config['default']['port']),
        'index_pattern': config['default'].get('index_pattern', 'filebeat-*')
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
    username = input("username: ")
    password = input("password: ")
    scheme = input("scheme (HIGLY recommended https): ")
    port = input("port: ")
    index_pattern = input("index pattern (default: filebeat-*): ") or "filebeat-*"

    config = configparser.RawConfigParser()
    config.add_section("default")
    config.set("default", "host", host)
    config.set("default", "username", username)
    config.set("default", "password", password)
    config.set("default", "scheme", scheme)
    config.set("default", "port", port)
    config.set("default", "index_pattern", index_pattern)

    with open(config_path, 'w') as configfile:
        config.write(configfile)

    print("elktail configured")
