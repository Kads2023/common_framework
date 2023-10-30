from abc import ABC
from validation_framework.common_validate.utils.config_utils import ConfigLoader
from validation_framework.common_validate.objects.input_args import InputArgs
from validation_framework.common_validate.utils import global_constants
from validation_framework.common_validate.datacompare.data_compare import DataCompare
from validation_framework.common_validate.countcompare.count_compare import CountCompare
from validation_framework.common_validate.endpoints.end_point import EndPoint

import os
import datetime
import time
import json


class JobParam(ABC):
    print("********** Inside JobParam ********** ")
    local_params: dict = {}
    table_1_endpoint: EndPoint
    table_2_endpoint: EndPoint
    results_endpoint: EndPoint
    data_compy: DataCompare
    count_compare: CountCompare
    spark_session = None
    env = os.getenv('ENVIRONMENT')
    home_base = os.getenv('HOME')
    environment = ""
    cluster = ""
    failed_flag = False
    passed_as_params = False
    failed_msg = ""
    metadata_connection_dict = {}

    if env == 'QA':
        environment = 'QA'
        cluster = 'POLARIS'
    elif env == 'SB':
        environment = 'SB'
        cluster = 'BERRYESSA'
    elif env == 'GCP':
        environment = 'GCP'
        cluster = 'CLOUD'
    else:
        environment = 'PROD'
        cluster = 'CLOUD'

    def __init__(self, input_args: InputArgs):
        print("********** Inside JobParam's __init__ method ********** ")
        print(f"Inside JobParam's __init__ method, "
              f"input_args.args --> {input_args.args}")
        self.args = input_args.args
        self.set_params("args", self.args)

        self.tenant = str(self.args.tenant).strip()
        self.set_params("tenant", self.tenant)
        self.using_spark = self.args.using_spark
        self.set_params("using_spark", self.using_spark)
        self.using_data_compy = self.args.using_data_compy
        self.check_sample_data = self.args.check_sample_data
        self.limit_sample_data = self.args.limit_sample_data
        self.check_counts = self.args.check_counts
        self.using_temp_table = self.args.using_temp_table
        self.in_comparison_type = self.args.comparison_type
        self.run_summary_only = self.args.run_summary_only

        self.validation_key_name = str(self.args.validation_key_name).strip()
        self.set_params("validation_key_name", self.validation_key_name)

        self.validation_date = str(self.args.validation_date).strip()
        self.set_params("validation_date", self.validation_date)

        self.run_date = str(self.args.run_date).strip()
        self.set_params("run_date", self.run_date)

        self.display_credentials = self.args.display_credentials
        self.set_params("display_credentials", self.display_credentials)

        self.table_1_type = self.args.table_1_type
        self.set_params("table_1_type", self.table_1_type)
        self.table_2_type = self.args.table_2_type
        self.set_params("table_2_type", self.table_2_type)
        self.results_type = self.args.results_type
        self.set_params("results_type", self.results_type)
        self.validation_type = self.table_1_type + "_" + self.table_2_type
        self.set_params("validation_type", self.validation_type)
        self.validation_sub_type = str(self.args.validation_sub_type).strip()
        self.set_params("validation_sub_type", self.validation_sub_type)
        self.use_default_keys_file = self.args.use_default_keys_file

        self.keys_file = self.args.keys_file
        if self.keys_file:
            print(f"Inside JobParam's __init__ method, "
                  f"keys_file --> {self.keys_file}")
        else:
            if self.use_default_keys_file == "False":
                self.join_columns = self.args.join_columns
                self.comparison_criteria = self.args.comparison_criteria
                if not self.in_comparison_type:
                    self.now_comparison_type = global_constants.default_comparison_type
                else:
                    self.now_comparison_type = self.in_comparison_type
                self.sample_record_counts = self.args.sample_record_counts
                self.table_full_name_1 = self.args.table_full_name_1
                self.table_full_name_2 = self.args.table_full_name_2
                if self.table_full_name_1 and \
                        self.table_full_name_2 and \
                        self.join_columns and \
                        self.comparison_criteria and \
                        self.now_comparison_type:
                    self.passed_as_params = True
                    self.where_clause_to_be_used_for_counts = self.args.where_clause_to_be_used_for_counts
                    self.run_for_whole_table = self.args.run_for_whole_table
                    self.where_clause = self.args.where_clause

                    if self.where_clause:
                        self.where_clause_1 = self.args.where_clause
                        self.where_clause_2 = self.args.where_clause
                    else:
                        self.where_clause_1 = self.args.where_clause_1
                        self.where_clause_2 = self.args.where_clause_2

                    self.query_1 = self.args.query_1
                    self.query_2 = self.args.query_2

                    self.select_columns = self.args.select_columns
                    if self.select_columns:
                        self.select_columns_from_table_1 = self.args.select_columns
                        self.select_columns_from_table_2 = self.args.select_columns
                    else:
                        self.select_columns_from_table_1 = self.args.select_columns_from_table_1
                        self.select_columns_from_table_2 = self.args.select_columns_from_table_2

                    self.ignore_columns = self.args.ignore_columns
                    if self.ignore_columns:
                        self.ignore_columns_from_table_1 = self.args.ignore_columns
                        self.ignore_columns_from_table_2 = self.args.ignore_columns
                    else:
                        self.ignore_columns_from_table_1 = self.args.ignore_columns_from_table_1
                        self.ignore_columns_from_table_2 = self.args.ignore_columns_from_table_2

                    self.limit_samples_count = self.args.limit_samples_count
                    if self.limit_samples_count:
                        self.limit_samples_count_from_table_1 = self.args.limit_samples_count
                        self.limit_samples_count_from_table_2 = self.args.limit_samples_count
                    else:
                        self.limit_samples_count_from_table_1 = self.args.limit_samples_count_from_table_1
                        self.limit_samples_count_from_table_2 = self.args.limit_samples_count_from_table_2

                    self.special_columns = self.args.special_columns
                    if self.special_columns:
                        self.special_columns_from_table_1 = self.args.special_columns
                        self.special_columns_from_table_2 = self.args.special_columns
                    else:
                        self.special_columns_from_table_1 = self.args.special_columns_from_table_1
                        self.special_columns_from_table_2 = self.args.special_columns_from_table_2

                    self.date_time_columns = self.args.date_time_columns
                    if self.date_time_columns:
                        self.date_time_columns_from_table_1 = self.args.date_time_columns
                        self.date_time_columns_from_table_2 = self.args.date_time_columns
                    else:
                        self.date_time_columns_from_table_1 = self.args.date_time_columns_from_table_1
                        self.date_time_columns_from_table_2 = self.args.date_time_columns_from_table_2

                    self.date_type_group_by_keys = self.args.date_type_group_by_keys
                    self.other_group_by_keys = self.args.other_group_by_keys
                    self.type_code_columns = self.args.type_code_columns
                    self.amount_columns = self.args.amount_columns
                    self.check_null_columns = self.args.check_null_columns
                else:
                    error_msg = f"table_full_name_1 --> {self.table_full_name_1}, " \
                                f"table_full_name_2 --> {self.table_full_name_2}, " \
                                f"join_columns --> {self.join_columns}, " \
                                f"comparison_criteria --> {self.comparison_criteria}, " \
                                f"in_comparison_type --> {self.in_comparison_type}, " \
                                f"now_comparison_type --> {self.now_comparison_type}, " \
                                f"use_default_keys_file --> {self.use_default_keys_file}, " \
                                f"WHEN use_default_keys_file IS FALSE, " \
                                f"FULL TABLE NAMES, JOIN COLUMNS, " \
                                f"COMPARISON CRITERIA AND COMPARISON TYPE " \
                                f"ARE MANDATORY"
                    print(f"Inside JobParam's __init__ method, "
                          f"error_msg --> {error_msg}")
                    raise ValueError(error_msg)

        user_dir = str(self.args.user_dir).strip()
        self.user_dir = user_dir
        if not user_dir:
            user_dir = global_constants.user_dir.strip().format(tenant=self.tenant)
        self.set_params("user_dir", user_dir)

        final_home = self.home_base + self.user_dir
        self.home = final_home
        self.set_params("home", self.home)
        self.config_file = self.args.config_file
        self.config_file_name = self.args.config_file_name
        self.set_params("config_dir", self.config_file)
        print(f"Inside JobParam's __init__ method, "
              f"self.args --> {self.get_params('args')}, "
              f"self.tenant --> {self.get_params('tenant')}, "
              f"self.config_dir --> {self.get_params('config_dir')}, "
              f"self.validation_type --> {self.get_params('validation_type')}, "
              f"self.validation_sub_type --> {self.get_params('validation_sub_type')}, "
              f"self.user_dir --> {self.get_params('user_dir')}, "
              f"self.env --> {self.env}, "
              f"self.home --> {self.home}")

        self.start_time = datetime.datetime.now().strftime(
            global_constants.timestamp_standard_format
        )
        time.sleep(1)
        self.set_params("cluster", self.cluster)
        self.set_params("environment", self.environment)
        self.job_initiation_time = datetime.datetime.now().strftime(
            global_constants.timestamp_standard_format
        )

    def set_common_config(self):
        print("********** Inside JobParam's set_common_config method ********** ")
        config_dir = self.get_params("config_dir").format(home_dir=self.home,
                                                                      tenant=self.tenant)
        print(f"Inside JobParam's set_common_config method, "
              f"config_dir --> {config_dir}")
        if self.config_file_name:
            if str(self.config_file_name).strip().endswith(".json"):
                config_dir = config_dir.replace("config.json", f"{self.config_file_name}")
            else:
                config_dir = config_dir.replace("config.json", f"{self.config_file_name}.json")
            print(f"Inside JobParam's set_common_config method, "
                  f"After replacing "
                  f"config_file_name --> {self.config_file_name}, "
                  f"config_dir --> {config_dir}")
        self.set_params("config_dir", config_dir)
        validate_config = ConfigLoader().read_conf_file(config_dir)
        self.local_params.update(validate_config)
        print(f"Inside JobParam's set_common_config method, "
              f"validate_config --> {validate_config}")

    def check_table_keys(self):
        print("********** Inside JobParam's check_table_keys method ********** ")
        if self.passed_as_params:
            if self.join_columns and \
                    self.comparison_criteria and \
                    self.now_comparison_type:
                if self.table_full_name_1 and self.table_full_name_2:
                    print(f"Inside JobParam's check_table_keys method, "
                          f"table_full_name_1 --> {self.table_full_name_1}, "
                          f"table_full_name_2 --> {self.table_full_name_2}, "
                          f"CHECKING TABLE WISE DETAILS")
                    if self.query_1 or self.where_clause_1:
                        print(f"Inside JobParam's check_table_keys method, "
                              f"query_1 --> {self.query_1}, "
                              f"where_clause_1 --> {self.where_clause_1}, "
                              f"ONE OF QUERY OR WHERE CLAUSE AVAILABLE, "
                              f"WE ARE GOOD WITH TABLE_1, "
                              f"table_full_name_1 --> {self.table_full_name_1}")
                    else:
                        error_msg = f"Inside JobParam's check_table_keys method, " \
                                    f"table_full_name_1 --> {self.table_full_name_1}, " \
                                    f"query_1 --> {self.query_1}, " \
                                    f"where_clause_1 --> {self.where_clause_1}, " \
                                    f"ONE OF QUERY OR WHERE CLAUSE " \
                                    f"SHOULD BE AVAILABLE, PLEASE CHECK"
                        print(f"error_msg --> {error_msg}")
                        raise ValueError(error_msg)
                    if self.query_2 or self.where_clause_2:
                        print(f"Inside JobParam's check_table_keys method, "
                              f"query_2 --> {self.query_2}, "
                              f"where_clause_2 --> {self.where_clause_2}, "
                              f"ONE OF QUERY OR WHERE CLAUSE AVAILABLE, "
                              f"WE ARE GOOD WITH TABLE_2, "
                              f"table_full_name_2 --> {self.table_full_name_2}")
                    else:
                        error_msg = f"Inside JobParam's check_table_keys method, " \
                                    f"table_full_name_1 --> {self.table_full_name_1}, " \
                                    f"query_1 --> {self.query_1}, " \
                                    f"where_clause_1 --> {self.where_clause_1}, " \
                                    f"ONE OF QUERY OR WHERE CLAUSE " \
                                    f"SHOULD BE AVAILABLE, PLEASE CHECK"
                        print(f"error_msg --> {error_msg}")
                        raise ValueError(error_msg)

                else:
                    error_msg = f"Inside JobParam's check_table_keys method, " \
                                f"table_full_name_1 --> {self.table_full_name_1}, " \
                                f"table_full_name_2 --> {self.table_full_name_2}, " \
                                f"join_columns --> {self.join_columns}, " \
                                f"comparison_criteria --> {self.comparison_criteria}, " \
                                f"in_comparison_type --> {self.in_comparison_type}, " \
                                f"now_comparison_type --> {self.now_comparison_type}, " \
                                f"use_default_keys_file --> {self.use_default_keys_file}, " \
                                f"WHEN use_default_keys_file IS FALSE, " \
                                f"FULL TABLE NAMES, JOIN COLUMNS, " \
                                f"COMPARISON CRITERIA AND COMPARISON TYPE " \
                                f"ARE MANDATORY, " \
                                f"FULL TABLE NAMES NOT AVAILABLE, PLEASE CHECK"
                    print(f"error_msg --> {error_msg}")
                    raise ValueError(error_msg)
            else:
                error_msg = f"Inside JobParam's check_table_keys method, " \
                            f"table_full_name_1 --> {self.table_full_name_1}, " \
                            f"table_full_name_2 --> {self.table_full_name_2}, " \
                            f"join_columns --> {self.join_columns}, " \
                            f"comparison_criteria --> {self.comparison_criteria}, " \
                                f"in_comparison_type --> {self.in_comparison_type}, " \
                                f"now_comparison_type --> {self.now_comparison_type}, " \
                            f"use_default_keys_file --> {self.use_default_keys_file}, " \
                            f"WHEN use_default_keys_file IS FALSE, " \
                            f"FULL TABLE NAMES, JOIN COLUMNS, " \
                            f"COMPARISON CRITERIA AND COMPARISON TYPE " \
                            f"ARE MANDATORY, " \
                            f"PLEASE CHECK"
                print(f"error_msg --> {error_msg}")
                raise ValueError(error_msg)
        else:
            print(f"Inside JobParam's check_table_keys method, "
                  f"passed_as_params --> {self.passed_as_params}, "
                  f"hence will be reading from keys.json")

    def set_table_keys(self):
        print("********** Inside JobParam's set_table_config method ********** ")
        in_keys_dir = self.args.keys_file
        validate_keys = {}
        now_keys = {}
        if in_keys_dir:
            keys_dir = in_keys_dir.format(
                home_dir=self.home,
                tenant=self.tenant
            )
            validate_keys = ConfigLoader().read_conf_file(keys_dir)
        else:
            if self.use_default_keys_file == "True":
                keys_dir = global_constants.keys_file_location.format(
                    home_dir=self.home,
                    tenant=self.tenant
                )
                validate_keys = ConfigLoader().read_conf_file(keys_dir)
            else:
                if self.passed_as_params:
                    comparison_type_from_config = self.safe_get_params(
                        global_constants.common_section + ",COMPARISON_TYPE"
                    )
                    if not self.in_comparison_type:
                        if not comparison_type_from_config:
                            final_comparison_type = global_constants.default_comparison_type
                        else:
                            final_comparison_type = comparison_type_from_config
                    else:
                        final_comparison_type = self.in_comparison_type
                    print(f"Inside JobParam's set_table_keys method, "
                          f"in_comparison_type --> {self.in_comparison_type}, "
                          f"comparison_type_from_config --> {comparison_type_from_config}, "
                          f"default_comparison_type --> {global_constants.default_comparison_type}, "
                          f"final_comparison_type --> {final_comparison_type}")
                    keys_dir = ""
                    now_keys["SAMPLE_RECORD_COUNT"] = self.sample_record_counts
                    if self.join_columns:
                        now_keys["JOIN_KEYS"] = str(self.join_columns).strip().split(';')
                    now_keys["COMPARISON_CRITERIA"] = str(self.comparison_criteria).strip()
                    now_keys["COMPARISON_TYPE"] = str(final_comparison_type).strip()
                    if self.date_type_group_by_keys:
                        now_keys["DATE_TYPE_GROUP_BY_KEYS"] = str(
                            self.date_type_group_by_keys).strip().split(';')
                    if self.other_group_by_keys:
                        now_keys["OTHER_GROUP_BY_KEYS"] = str(
                            self.other_group_by_keys).strip().split(';')
                    if self.where_clause_to_be_used_for_counts:
                        now_keys["WHERE_CLAUSE_TO_BE_USED_FOR_COUNTS"] = str(
                            self.where_clause_to_be_used_for_counts).strip()
                    if self.run_for_whole_table:
                        now_keys["RUN_FOR_WHOLE_TABLE"] = str(
                            self.run_for_whole_table).strip()
                    if self.amount_columns:
                        now_keys["AMOUNT_COLUMNS"] = str(self.amount_columns).strip().split(';')
                    if self.type_code_columns:
                        now_keys["TYPE_CODE_COLUMNS"] = str(self.type_code_columns).strip().split(';')
                    if self.check_null_columns:
                        now_keys["CHECK_NULL_COLUMNS"] = str(self.check_null_columns).strip().split(';')

                    table_1_details = {
                        "FULL_TABLE_NAME": f"{self.table_full_name_1}",
                        "WHERE_CLAUSE": f"{self.where_clause_1}",
                        "QUERY": f"{self.query_1}"
                    }
                    table_2_details = {
                        "FULL_TABLE_NAME": f"{self.table_full_name_2}",
                        "WHERE_CLAUSE": f"{self.where_clause_2}",
                        "QUERY": f"{self.query_2}"
                    }
                    if self.select_columns_from_table_1:
                        table_1_details["SELECT_COLUMNS"] = str(self.select_columns_from_table_1
                                                                ).strip().split(';')
                    if self.select_columns_from_table_2:
                        table_2_details["SELECT_COLUMNS"] = str(self.select_columns_from_table_2
                                                                ).strip().split(';')
                    if self.ignore_columns_from_table_1:
                        table_1_details["IGNORE_COLUMNS"] = str(self.ignore_columns_from_table_1
                                                                ).strip().split(';')
                    if self.ignore_columns_from_table_2:
                        table_2_details["IGNORE_COLUMNS"] = str(self.ignore_columns_from_table_2
                                                                ).strip().split(';')
                    if self.limit_samples_count_from_table_1:
                        table_1_details["LIMIT_SAMPLE"] = str(self.limit_samples_count_from_table_1
                                                              ).strip()
                    if self.limit_samples_count_from_table_2:
                        table_2_details["LIMIT_SAMPLE"] = str(self.limit_samples_count_from_table_2
                                                              ).strip()
                    if self.special_columns_from_table_1:
                        table_1_details["SPECIAL_COLUMNS"] = json.loads(str(self.special_columns_from_table_1
                                                                            ).strip())
                    if self.special_columns_from_table_2:
                        table_2_details["SPECIAL_COLUMNS"] = json.loads(str(self.special_columns_from_table_2
                                                                            ).strip())
                    if self.date_time_columns_from_table_1:
                        table_1_details["DATE_TIME_COLUMNS"] = str(self.date_time_columns_from_table_1
                                                                   ).strip().split(';')
                    if self.date_time_columns_from_table_2:
                        table_2_details["DATE_TIME_COLUMNS"] = str(self.date_time_columns_from_table_2
                                                                   ).strip().split(';')
                    now_keys["TABLE_1_DETAILS"] = table_1_details
                    now_keys["TABLE_2_DETAILS"] = table_2_details
                    validate_keys[self.validation_key_name] = now_keys
                else:
                    keys_dir = self.get_params(global_constants.keys_dir_key_name.strip())
                    validate_keys = ConfigLoader().read_conf_file(keys_dir)
        self.local_params.update(validate_keys)
        print(f"Inside JobParam's set_table_keys method, "
              f"validate_keys --> {validate_keys}")
        validation_key_name_keys = validate_keys[self.validation_key_name]
        table_full_name_1 = validation_key_name_keys["TABLE_1_DETAILS"]["FULL_TABLE_NAME"]
        self.set_params("table_full_name_1", table_full_name_1)
        table_full_name_2 = validation_key_name_keys["TABLE_2_DETAILS"]["FULL_TABLE_NAME"]
        self.set_params("table_full_name_2", table_full_name_2)
        self.set_params("keys_dir", keys_dir)

    def override_params_from_config(self):
        print("********** Inside JobParam's override_params_from_config method ********** ")
        fin_base_path = global_constants.base_path
        base_path = self.safe_get_params(
            global_constants.common_section.strip() +
            "," + global_constants.base_path_key_name.strip()).\
            format(home_dir=self.home,
                   tenant=self.tenant)
        if base_path != "":
            fin_base_path = base_path
        self.set_params(global_constants.base_path_key_name.strip(), fin_base_path)

        fin_common_base_path = global_constants.common_base_path
        common_base_path = self.safe_get_params(
            global_constants.common_section.strip() +
            "," + global_constants.common_base_path_key_name.strip()).\
            format(home_dir=self.home,
                   base_path=fin_base_path,
                   tenant=self.tenant)
        if common_base_path != "":
            fin_common_base_path = common_base_path
        self.set_params(global_constants.common_base_path_key_name.strip(), fin_common_base_path)

        fin_xp_base_path = global_constants.xp_base_path
        xp_base_path = self.safe_get_params(
            global_constants.common_section.strip() +
            "," + global_constants.xp_base_path_key_name.strip()).\
            format(home_dir=self.home,
                   base_path=fin_base_path,
                   tenant=self.tenant)
        if xp_base_path != "":
            fin_xp_base_path = xp_base_path
        self.set_params(global_constants.xp_base_path_key_name.strip(), fin_xp_base_path)
        self.set_params("system_timezone", self.get_params(global_constants.common_section.strip() +
                                                           ",DATA,SYSTEM_TIMEZONE"))
        if self.using_data_compy:
            self.set_params(f"{global_constants.common_section},USING_DATA_COMPY",
                            self.using_data_compy)
        if self.check_sample_data:
            self.set_params(f"{global_constants.common_section},CHECK_SAMPLE_DATA",
                            self.check_sample_data)
        if self.limit_sample_data:
            self.set_params(f"{global_constants.common_section},LIMIT_SAMPLE_DATA",
                            self.limit_sample_data)
        if self.check_counts:
            self.set_params(f"{global_constants.common_section},CHECK_COUNTS",
                            self.check_counts)
        if self.using_temp_table:
            self.set_params(f"{global_constants.common_section},USING_TEMP_TABLE",
                            self.using_temp_table)
        if self.in_comparison_type:
            self.set_params(f"{global_constants.common_section},COMPARISON_TYPE",
                            self.in_comparison_type)
        if self.run_summary_only:
            self.set_params(f"{global_constants.common_section},RUN_SUMMARY_ONLY",
                            self.run_summary_only)
        print(f"Inside JobParam's override_params_from_config method, "
              f"local_params --> {self.local_params}")

    def set_params(self, passed_key_name: str, passed_value):
        keys = passed_key_name.split(",")
        length_of_keys = len(keys)
        if length_of_keys == 1:
            self.local_params[passed_key_name] = passed_value
        elif length_of_keys == 2:
            self.local_params[keys[0].strip()][keys[1].strip()] = passed_value
        elif length_of_keys == 3:
            self.local_params[keys[0].strip()][keys[1].strip()][keys[2].strip()] = passed_value

    def safe_get_params(self, func_key_name: str, default_value=""):
        safe_return_val = default_value
        try:
            safe_return_val = self.get_params(func_key_name)
        except KeyError as ke:
            print(f"Inside JobParam's safe_get_params method, "
                  f"passed key --> {func_key_name}, "
                  f"does not exists, got the exception --> {ke}")
        return safe_return_val

    def get_params(self, passed_key_name: str, default_value=""):
        return_val = default_value
        keys = passed_key_name.split(",")
        length_of_keys = len(keys)
        if length_of_keys == 1:
            return_val = self.local_params[passed_key_name]
        elif length_of_keys == 2:
            return_val = self.local_params[keys[0].strip()][keys[1].strip()]
        elif length_of_keys == 3:
            return_val = self.local_params[keys[0].strip()][keys[1].strip()][keys[2].strip()]
        return return_val
