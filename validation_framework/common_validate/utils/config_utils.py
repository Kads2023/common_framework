import json


class ConfigLoader:
    __config_dict = {}

    def read_conf_file(self, config_file_path) -> dict:
        try:
            print("********** Inside ConfigLoader's read_conf_file method ********** ")
            print(f"config_file_path --> {config_file_path}")
            with open('{}'.format(config_file_path)) as config:
                self.__config_dict = json.load(config)
            config.close()
            print(f"config_dic --> {self.__config_dict}")
            return self.__config_dict
        except Exception as e:
            print(f"********** Exception.ConfigLoader.read_conf_file {e} ********** ")
            raise e
