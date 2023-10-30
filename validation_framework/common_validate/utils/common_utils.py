from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.logger_utils import LoggerUtils
from validation_framework.common_validate.utils import global_constants
import subprocess
from subprocess import PIPE
import datetime
import pytz


class CommonOperations:
    def __init__(self, passed_logger: LoggerUtils, passed_job_params: JobParam):
        self.__logger: LoggerUtils = passed_logger
        self.__job_params = passed_job_params
        self.__base_path = self.__job_params.get_params(global_constants.base_path_key_name)
        self.__xp_base_path = self.__job_params.get_params(global_constants.xp_base_path_key_name)
        self.__common_base_path = self.__job_params.get_params(global_constants.common_base_path_key_name)
        self.__tenant = self.__job_params.tenant

        self.__display_data = self.safe_get_params(
            global_constants.common_section + ",DISPLAY_DATA"
        )

    def check_skip_job(self):
        skip_job = self.__job_params.get_params(
            global_constants.common_section.strip() +
            "," + global_constants.skip_job_file.strip()).\
            format(base_path=self.__base_path,
                   common_base_path=self.__common_base_path,
                   xp_base_path=self.__xp_base_path,
                   tenant=self.__tenant)
        now_validation_key_name = str(
            self.__job_params.get_params("validation_key_name")
        ).strip().lower()
        self.log_and_print(f"In check_skip_job of CommonOperations, "
                           f"skip_job --> {skip_job}, "
                           f"now_validation_key_name --> {now_validation_key_name}",
                           print_msg=True)
        with open(skip_job, 'r') as fileobject:
            lines = fileobject.read().splitlines()
            for key_name_item in lines:
                if key_name_item == now_validation_key_name:
                    self.log_and_print(f'In check_skip_job of CommonOperations, '
                                       f'ob found in skip list '
                                       f'{now_validation_key_name}')
                    exit(0)
                else:
                    continue
            self.log_and_print("In check_skip_job of CommonOperations, "
                               "Job not in skip list, "
                               "Continuing with the job")
        fileobject.close()

    def epoch_to_date(self, epoch_time_ms, passed_timezone=None, passed_format=None):
        self.log_and_print(f"In epoch_to_date of CommonOperations, "
                           f"epoch_time_ms --> {epoch_time_ms}, "
                           f"passed_timezone --> {passed_timezone}, "
                           f"passed_format --> {passed_format}")

        pytz_timezone = passed_timezone or global_constants.default_timezone
        date_format = passed_format or global_constants.timestamp_standard_format

        timestamp = (float(epoch_time_ms) / 1000.0)
        run_id = datetime.datetime.fromtimestamp(
            timestamp,
            tz=pytz.timezone(pytz_timezone)
        ).strftime(date_format)
        self.log_and_print(f"In epoch_to_date of CommonOperations, "
                           f"epoch_time_ms --> {epoch_time_ms}, "
                           f"pytz_timezone --> {pytz_timezone}, "
                           f"run_id --> {run_id}")
        return run_id

    def log_and_print(self, message, logger_type=None, print_msg=False):
        if logger_type is not None and not print_msg:
            final_logger_type = str(logger_type)
            if final_logger_type != "info" or final_logger_type != "debug":
                print_msg = True
        if print_msg:
            print(message)
        self.__logger.log_passed_msg(message, logger_type)

    def display_data_frame(self, passed_dataframe, passed_name_of_dataframe):
        self.log_and_print(f"In display_data_frame of CommonOperations, "
                           f"passed_name_of_dataframe --> {passed_name_of_dataframe}")
        if self.__display_data == "True":
            passed_dataframe.show(n=10, truncate=False)

    def list_of_tuples_to_dict(self, passed_list_of_tuples):
        self.log_and_print(f"In list_of_tuples_to_dict of CommonOperations, "
                           f"passed_list_of_tuples --> {passed_list_of_tuples}")
        ret_dict = {}
        for each_tuple in passed_list_of_tuples:
            ret_dict[each_tuple[0]] = each_tuple[1]
        self.log_and_print(f"In list_of_tuples_to_dict of CommonOperations, "
                           f"ret_dict --> {ret_dict}")
        return ret_dict

    def get_pandas_df_schema(self, columns_list, data_types_list):
        self.log_and_print(f"In get_pandas_df_schema of CommonOperations, "
                           f"columns_list --> {columns_list}, "
                           f"data_types_list --> {data_types_list}")
        from pyspark.sql.types import StructType, StructField, \
            StringType, TimestampType, LongType, \
            IntegerType, DoubleType, FloatType
        struct_list = []
        for each_column, each_type in zip(columns_list, data_types_list):
            now_type = StringType()
            if each_type == 'datetime64[ns]':
                now_type = TimestampType()
            elif each_type == 'int64':
                now_type = LongType()
            elif each_type == 'int32':
                now_type = IntegerType()
            elif each_type == 'float64':
                now_type = DoubleType()
            elif each_type == 'float32':
                now_type = FloatType()
            struct_list.append(StructField(each_column, now_type))
        p_schema = StructType(struct_list)
        self.log_and_print(f"In get_pandas_df_schema of CommonOperations, "
                           f"p_schema --> {p_schema}")
        return p_schema

    def get_validation_type_keys(self, passed_endpoint_name, passed_endpoint_type):
        self.log_and_print(f"In get_validation_type_keys of CommonOperations, "
                           f"In get_validation_type_keys of CommonOperations, "
                           f"passed_endpoint_name --> {passed_endpoint_name}, "
                           f"passed_endpoint_type --> {passed_endpoint_type}")
        final_now_validation_type_keys = {}
        now_base_validation_type_keys = {}
        validation_type = str(
            self.__job_params.get_params("validation_type")
        ).strip().upper()
        validation_sub_type = str(
            self.safe_get_params("validation_sub_type")
        ).strip().upper()
        final_passed_endpoint_name = str(passed_endpoint_name).strip().upper()
        now_validation_type_keys = self.safe_get_params(
            validation_type + "," +
            final_passed_endpoint_name + "_CONFIG"
        ) or {}
        now_validation_sub_type_keys = {}
        if validation_sub_type:
            now_validation_sub_type_keys = self.safe_get_params(
                validation_type + "," +
                validation_sub_type + "_CONFIG" + "," +
                final_passed_endpoint_name + "_CONFIG"
            ) or {}
        if now_validation_sub_type_keys:
            if passed_endpoint_type.strip().lower() == "results":
                now_base_validation_type_keys = now_validation_sub_type_keys
                final_now_validation_type_keys = \
                    now_validation_sub_type_keys.get("RESULTS", {})
            else:
                now_base_validation_type_keys = \
                    final_now_validation_type_keys = now_validation_sub_type_keys
        elif now_validation_type_keys:
            if passed_endpoint_type.strip().lower() == "results":
                now_base_validation_type_keys = now_validation_type_keys
                final_now_validation_type_keys = now_validation_type_keys.get("RESULTS", {})
            else:
                now_base_validation_type_keys = \
                    final_now_validation_type_keys = now_validation_type_keys
        else:
            error_msg = "In get_validation_type_keys of CommonOperations, " \
                        f"passed_endpoint_name --> {passed_endpoint_name}, " \
                        f"passed_endpoint_type --> {passed_endpoint_type}, " \
                        f"validation_type --> {validation_type}, " \
                        f"validation_sub_type --> {validation_sub_type}, " \
                        f"now_validation_type_keys --> {now_validation_type_keys}, " \
                        f"now_validation_sub_type_keys --> {now_validation_sub_type_keys}, " \
                        f"No keys found or empty"
            self.raise_value_error(error_msg)
        return now_base_validation_type_keys, final_now_validation_type_keys

    def append_string(self, base_str, append_str, delimiter="", prefix=""):
        self.log_and_print(f"In append_string of CommonOperations, "
                           f"base_str --> {base_str}, "
                           f"append_str --> {append_str}, "
                           f"delimiter --> {delimiter}, "
                           f"prefix --> {prefix}")
        if base_str:
            if str(append_str).startswith(";"):
                return_str = f"{base_str}{append_str}"
            else:
                if not delimiter:
                    return_str = f"{base_str}, {append_str}"
                else:
                    return_str = f"{base_str}{delimiter}{append_str}"
        else:
            if not prefix:
                return_str = append_str
            else:
                return_str = f"{prefix}{append_str}"
        self.log_and_print(f"In append_string of CommonOperations, "
                           f"return_str --> {return_str}")
        return return_str

    def time_diff(self, old_time):
        now_time = datetime.datetime.now()
        diff_time = old_time - now_time
        self.log_and_print(f"old_time --> {old_time}, "
                           f"now_time --> {now_time}, "
                           f"diff_time --> {diff_time}")

    def raise_value_error(self, passed_error_msg):
        self.log_and_print(passed_error_msg, logger_type="error")
        self.__job_params.failed_flag = True
        existing_msg = self.__job_params.failed_msg
        final_msg = str(passed_error_msg)
        if existing_msg != "":
            final_msg = str(existing_msg) + ", " + str(passed_error_msg)
        self.__job_params.failed_msg = final_msg
        raise ValueError(passed_error_msg)

    def safe_get_params(self, key_name_passed, default_value=""):
        return_val = default_value
        try:
            return_val = self.__job_params.get_params(key_name_passed)
        except KeyError as ke:
            self.log_and_print(f"In safe_get_params of CommonOperations, "
                               f"passed key --> {key_name_passed}, "
                               f"does not exists, got the exception --> {ke}",
                               logger_type="error")
        return return_val

    def check_job_details_params(self, key_name):
        keys = key_name.split(",")
        length_of_keys = len(keys)
        if length_of_keys > 3:
            error_msg = f"In check_job_details_params of CommonOperations, " \
                        f"unknown depth of keys, " \
                        f"keys --> {key_name}, " \
                        f"len(keys) --> {length_of_keys}"
            self.raise_value_error(error_msg)

    def call_set_job_details_params(self, key_name, key_value, _check=True):
        keys = key_name.split(",")
        length_of_keys = len(keys)
        if length_of_keys > 3:
            error_msg = f"In call_set_job_details_params of CommonOperations, " \
                        f"unknown depth of keys, " \
                        f"keys --> {key_name}, " \
                        f"len(keys) --> {length_of_keys}, " \
                        f"passed_value --> {key_value} "
            self.raise_value_error(error_msg)
        self.__job_params.set_params(key_name, key_value)
        if _check:
            self.check_job_details_params(key_name)

    def exec_command(self, command):
        self.log_and_print(f"In exec_command of CommonOperations, "
                           f"command --> {command}")
        self.log_and_print('Running system command: {0}\n'.format(''.join(command)))
        pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        stdo, stde = pobj.communicate()
        exit_code = pobj.returncode
        if stdo or stde:
            self.log_and_print(f"stdo --> {stdo}, stde --> {stde}")
        return exit_code, stdo, stde
