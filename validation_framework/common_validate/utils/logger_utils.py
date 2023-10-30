"""
Used for logging related modules
"""

import logging
import os
import sys
from enum import Enum

from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils import global_constants


class LoggerUtils:
    """
    class holding modules related to logging
    """
    __logger = None
    __formatter = None
    __formatter_values = None

    class LogMode(Enum):
        FILE = "file"
        CONSOLE = "console"

    def __init__(self, passed_job_params: JobParam):
        self.__tenant = passed_job_params.get_params("tenant")
        self.__validation_key_name = passed_job_params.get_params("validation_key_name")
        self.__validation_type = passed_job_params.get_params("validation_type")
        self.__validation_sub_type = passed_job_params.get_params("validation_sub_type")
        self.__base_path = passed_job_params.get_params(global_constants.base_path_key_name)
        self.__xp_base_path = passed_job_params.get_params(global_constants.xp_base_path_key_name)
        self.__common_base_path = passed_job_params.get_params(global_constants.common_base_path_key_name)
        self.__log_mode = passed_job_params.get_params(f"{global_constants.common_section},{global_constants.log_mode}")

        self.__formatter_values = f"{self.__tenant}:{self.__validation_type}:{self.__validation_sub_type}"
        self.__formatter = logging.Formatter('%(asctime)s | %(levelname)s | {' + self.__formatter_values +
                                             '} | %(message)s')

        if self.__log_mode == LoggerUtils.LogMode.FILE.value:
            self.__log_file_location = passed_job_params.get_params(
                global_constants.common_section + "," +
                global_constants.log_file_path_key_name). \
                format(base_path=self.__base_path,
                       common_base_path=self.__common_base_path,
                       xp_base_path=self.__xp_base_path,
                       tenant=self.__tenant,
                       run_date=global_constants.date_str)
            print(f"log_file_location --> {self.__log_file_location}")
            if not os.path.exists(self.__log_file_location):
                os.makedirs(self.__log_file_location)
            passed_job_params.set_params("log_file_location", self.__log_file_location)

            self.__log_file_pattern = passed_job_params.get_params(
                global_constants.common_section + "," +
                global_constants.log_file_pattern_key_name). \
                format(tenant=self.__tenant,
                       validation_key_name=self.__validation_key_name,
                       validation_type=self.__validation_type,
                       validation_sub_type=self.__validation_sub_type,
                       run_date=global_constants.date_str,
                       date_time_str=global_constants.date_time_str
                       )

            self.__log_handlers = passed_job_params.get_params(
                global_constants.common_section + "," +
                global_constants.log_handlers_key_name).split(",")

    def add_logger(self, passed_type):
        """
        used to add logger based on the passed type
        """
        now_type = passed_type.lower()
        now_final_log_file_name = self.__log_file_pattern + f'_{now_type}.log'
        now_final_log_file = self.__log_file_location + '/' + now_final_log_file_name
        print(f"now_final_log_file --> {now_final_log_file}")

        now_file_handler = logging.FileHandler(now_final_log_file, mode='w')
        now_file_handler.setFormatter(self.__formatter)

        if now_type == 'info':
            now_file_handler.setLevel(logging.INFO)
        elif now_type == 'debug':
            now_file_handler.setLevel(logging.DEBUG)
        elif now_type == 'warning':
            now_file_handler.setLevel(logging.WARNING)
        elif now_type == 'error':
            now_file_handler.setLevel(logging.ERROR)
        elif now_type == 'critical':
            now_file_handler.setLevel(logging.CRITICAL)
        else:
            now_file_handler.setLevel(logging.INFO)

        self.__logger.addHandler(now_file_handler)

    def create_logger(self):
        """
        used to create the logger object if not exists
        """
        self.__logger = logging.getLogger(__name__)

        if self.__log_mode == LoggerUtils.LogMode.FILE.value:
            if not os.path.exists(self.__log_file_location):
                os.makedirs(self.__log_file_location)

            for each_handler in self.__log_handlers:
                self.add_logger(str(each_handler).strip())
        else:
            now_stream_handler = logging.StreamHandler(sys.stdout)
            now_stream_handler.setFormatter(self.__formatter)
            now_stream_handler.setLevel(logging.DEBUG)
            self.__logger.addHandler(now_stream_handler)

        self.__logger.setLevel(logging.DEBUG)
        self.__logger.info("Created logger")

    def log_passed_msg(self, passed_msg, passed_logger_type=None):
        try:
            final_logger_type = str(passed_logger_type).strip().lower()
            if final_logger_type == 'error':
                self.__logger.error(passed_msg)
            elif final_logger_type == 'debug':
                self.__logger.debug(passed_msg)
            elif final_logger_type == 'warn' or final_logger_type == 'warning':
                self.__logger.warning(passed_msg)
            elif final_logger_type == 'critical':
                self.__logger.critical(passed_msg)
            else:
                self.__logger.info(passed_msg)
        except Exception as ex:
            print(f"inside log_and_print, "
                  f"encountered Exception --> {ex}, "
                  f"while logging message --> {passed_msg}, "
                  f"logger_type --> {passed_logger_type}")

    def upload_log_file(self, passed_end_point_object, passed_target_location):
        """
        used to upload the log files to the target location from the VM
        """
        log_files = self.__log_file_location + '/' + self.__log_file_pattern + '*.log'
        final_log_file = self.__log_file_pattern
        final_log_location = passed_target_location + '/' + final_log_file

        passed_end_point_object.move_file(log_files, final_log_location)
