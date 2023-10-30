import configparser


class ConfigReader:
    def __init__(self, passed_config_type):
        self.config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        self.config.read(f'../resources/{str(passed_config_type).upper()}/config.ini')
