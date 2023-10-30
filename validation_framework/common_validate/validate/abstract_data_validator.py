from abc import ABC, abstractmethod
from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.logger_utils import LoggerUtils
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.utils.keymaker_utils import KeyMakerApiProxy
from validation_framework.common_validate.endpoints.endpoint_factory import EndpointFactory
from validation_framework.common_validate.datacompare.datacompare_factory import DataCompareFactory
from validation_framework.common_validate.countcompare.countcompare_factory import CountCompareFactory

from validation_framework.common_validate.utils import global_constants


class ADataValidator(ABC):

    def __init__(self, job_params: JobParam):
        print("********** Inside ADataValidator's [validation_framework's] "
              "[CommonLoad's] __init__ method ********** ")
        self.job_params = job_params
        self.job_params.set_common_config()
        self.job_params.override_params_from_config()
        self.job_params.check_table_keys()
        self.job_params.set_table_keys()
        self.logger = LoggerUtils(self.job_params)
        self.logger.create_logger()
        self.common_operations = CommonOperations(self.logger, self.job_params)
        self.common_operations. \
            log_and_print("********** Inside ADataLoader's [CommonLoad's] [CommonLoad's] __init__ method ********** ",
                          print_msg=True)
        self.key_maker = KeyMakerApiProxy(self.job_params,
                                          self.common_operations)
        self.table_full_name_1 = self.job_params.get_params("table_full_name_1")
        self.table_full_name_2 = self.job_params.get_params("table_full_name_2")
        self.check_counts = self.job_params.safe_get_params(
            global_constants.common_section + ",CHECK_COUNTS"
        )
        self.check_sample_data = self.job_params.safe_get_params(
            global_constants.common_section + ",CHECK_SAMPLE_DATA"
        )

    @abstractmethod
    def pre_load(self):
        self.common_operations. \
            log_and_print("********** Inside ADataValidator's [validation_framework's] "
                          "[CommonLoad's] pre_load method ********** ")
        table_1_type = self.job_params.get_params("table_1_type")
        table_2_type = self.job_params.get_params("table_2_type")
        results_type = self.job_params.get_params("results_type")

        self.job_params.table_1_endpoint = EndpointFactory(self.job_params,
                                                           self.common_operations,
                                                           self.key_maker). \
            get_endpoint_connection(table_1_type, "table_1")
        self.job_params.table_2_endpoint = EndpointFactory(self.job_params,
                                                           self.common_operations,
                                                           self.key_maker). \
            get_endpoint_connection(table_2_type, "table_2")
        self.job_params.results_endpoint = EndpointFactory(self.job_params,
                                                           self.common_operations,
                                                           self.key_maker). \
            get_endpoint_connection(results_type, "results")
        self.common_operations.check_skip_job()

    @abstractmethod
    def load(self):
        self.common_operations. \
            log_and_print("********** Inside ADataValidator's [validation_framework's] "
                          "[CommonLoad's] load method ********** ")
        table_1_exists = self.job_params.table_1_endpoint.check_table()
        table_2_exists = self.job_params.table_2_endpoint.check_table()

        if table_1_exists and table_2_exists:
            self.common_operations.log_and_print(f"Inside ADataValidator's [validation_framework's] "
                                                 f"[CommonLoad's] load method, "
                                                 f"table_full_name_1 --> {self.table_full_name_1}, "
                                                 f"table_1_exists --> {table_1_exists}, "
                                                 f"table_full_name_2 --> {self.table_full_name_2}, "
                                                 f"table_2_exists --> {table_2_exists}")

            if self.check_counts == "True":
                self.job_params.count_compare = CountCompareFactory(
                    self.job_params,
                    self.common_operations,
                    self.key_maker
                ).get_count_compare()
            else:
                self.common_operations.log_and_print(f"Inside ADataValidator's "
                                                     f"[validation_framework's] "
                                                     f"[CommonLoad's] load method, "
                                                     f"check_counts --> "
                                                     f"{self.check_counts}, "
                                                     f"HENCE SKIPPING COUNTS CHECK")
            if self.check_sample_data == "True":
                self.job_params.data_compy = DataCompareFactory(
                    self.job_params,
                    self.common_operations,
                    self.key_maker
                ).get_data_compare()
            else:
                self.common_operations.log_and_print(f"Inside ADataValidator's "
                                                     f"[validation_framework's] "
                                                     f"[CommonLoad's] load method, "
                                                     f"check_sample_data --> "
                                                     f"{self.check_sample_data}, "
                                                     f"HENCE SKIPPING SAMPLE DATA CHECK")
        else:
            error_msg = f"In run_load of ADataValidator's [validation_framework's] " \
                        f"[CommonLoad's] load method, " \
                        f"table_full_name_1 --> {self.table_full_name_1}, " \
                        f"table_1_exists --> {table_1_exists}, " \
                        f"table_full_name_2 --> {self.table_full_name_2}, " \
                        f"table_2_exists --> {table_2_exists}, " \
                        f"{self.table_full_name_1} OR {self.table_full_name_2}, " \
                        f"DOES NOT EXISTS"
            self.common_operations.raise_value_error(error_msg)

    @abstractmethod
    def post_load(self):
        self.common_operations. \
            log_and_print("********** Inside ADataValidator's [validation_framework's] "
                          "[CommonLoad's] post_load method ********** ")
        self.job_params.table_1_endpoint.close_connection()
        self.job_params.table_2_endpoint.close_connection()
        self.job_params.results_endpoint.close_connection()

    def run_load(self):
        self.common_operations. \
            log_and_print("********** Inside ADataValidator's [validation_framework's] "
                          "[CommonLoad's] run_load method ********** ",
                          print_msg=True)
        try:
            self.pre_load()
            self.load()
        except Exception as e:
            error_msg = f"In run_load of ADataValidator's [validation_framework's] " \
                        f"[CommonLoad's] Got an exception while processing, "\
                        f"error_msg --> {e}"
            self.common_operations.log_and_print(error_msg, logger_type="error")
            if not self.job_params.failed_flag:
                self.job_params.failed_flag = True
            if self.job_params.failed_msg == "":
                self.job_params.failed_msg = error_msg
        finally:
            self.common_operations. \
                log_and_print(f"In run_load of ADataValidator's [validation_framework's] "
                              f"[CommonLoad's], "
                              f"inside finally", print_msg=True)
            self.post_load()
            self.common_operations. \
                log_and_print("********** Inside ADataValidator's [validation_framework's] "
                              "[CommonLoad's] run_load completed ********** ",
                              print_msg=True)
            if self.job_params.failed_flag:
                existing_msg = self.job_params.failed_msg
                self.common_operations.raise_value_error(existing_msg)
