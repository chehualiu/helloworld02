import configparser

def read_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    return config
