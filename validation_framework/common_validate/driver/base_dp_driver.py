import json
import os
import site

from validation_framework.common_validate.objects.input_args import InputArgs

from validation_framework.common_validate.utils import global_constants, retry
from validation_framework.common_validate.utils.config_utils import ConfigLoader

os.environ["HOME"] = site.getsitepackages()[0]
os.environ["ENVIRONMENT"] = "PROD"
os.environ["ORACLE_HOME"] = "<>"
os.environ["TNS_ADMIN"] = f"{os.getenv('ORACLE_HOME')}/network/admin"


class BaseDpDriver(object):

    def __init__(self, passed_input_params: InputArgs):
        print("********** Inside BaseDpDriver's __init__ method ********** ")
        self.apply_dp_configs(passed_input_params)

    def merge_json(self, config_json, dp_config_json):
        for dp_key, dp_value in dp_config_json.items():
            if isinstance(dp_value, dict):
                self.merge_json(config_json[dp_key], dp_value)
            else:
                config_json[dp_key] = dp_value

    @retry.retry()
    def apply_dp_configs(self, _input_params):
        home_dir = os.getenv('HOME')
        print(f"home_dir --> {home_dir}, "
              f"{os.getenv('ENVIRONMENT')}, {os.getenv('ORACLE_HOME')}")

        conf_location = _input_params.config_file or f"{global_constants.config_file_dir}" \
                                                           f"/config.json"
        config_loc = conf_location.format(home_dir=home_dir, tenant=_input_params.tenant)
        dp_config_dir = global_constants.config_file_dir.format(
            home_dir=home_dir,
            tenant=_input_params.tenant
        )

        config = ConfigLoader().read_conf_file(config_loc)
        dp_config = ConfigLoader().read_conf_file(f"{dp_config_dir}/dp_config.json")
        dp_config["COMMON_CONFIG"]["BASE_PATH"] = dp_config["COMMON_CONFIG"]["BASE_PATH"].format(home_dir=home_dir)

        # merging two dict to update dpp configs and replacing the config
        self.merge_json(config, dp_config)
        with open(config_loc, "w") as fp:
            json.dump(config, fp)
            _input_params.config_file = config_loc
            _input_params.args.config_file = config_loc
