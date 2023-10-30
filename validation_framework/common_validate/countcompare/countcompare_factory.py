from validation_framework.common_validate.countcompare.spark_count_compare_with_temp_table import SparkCountCompareWithTempTable
from validation_framework.common_validate.countcompare.spark_count_compare_without_temp_table import SparkCountCompareWithoutTempTable
from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.utils.keymaker_utils import KeyMakerApiProxy

from validation_framework.common_validate.utils import global_constants


class CountCompareFactory:

    def __init__(self,
                 passed_job_params: JobParam,
                 passed_common_operations: CommonOperations,
                 passed_keymaker: KeyMakerApiProxy
                 ):
        self.__job_params = passed_job_params
        self.__common_operations = passed_common_operations
        self.__keymaker = passed_keymaker
        self.__using_spark = self.__job_params.get_params("using_spark")
        self.__using_temp_table = self.__job_params.safe_get_params(
            global_constants.common_section + ",USING_TEMP_TABLE"
        )

    def get_count_compare(self):
        self.__common_operations.\
            log_and_print("********** In get_count_compare of CountCompareFactory ********** ")
        if self.__using_spark == "True":
            self.__common_operations.\
                log_and_print(f"********** In get_count_compare of CountCompareFactory, "
                              f"using_spark --> {self.__using_spark}, "
                              f"using_temp_table --> "
                              f"{self.__using_temp_table} ********** ")
            if self.__using_temp_table == "True":
                self.__common_operations. \
                    log_and_print(f"********** In get_count_compare of CountCompareFactory, "
                                  f"getting SparkCountCompareWithTempTable ********** ")
                return SparkCountCompareWithTempTable(
                    self.__job_params,
                    self.__common_operations
                )
            else:
                self.__common_operations. \
                    log_and_print(f"********** In get_count_compare of CountCompareFactory, "
                                  f"getting SparkCountCompareWithoutTempTable ********** ")
                return SparkCountCompareWithoutTempTable(
                    self.__job_params,
                    self.__common_operations
                )
        else:
            self.__common_operations.\
                log_and_print("we do not have the CountCompare implementation, "
                              f"using_spark --> {self.__using_spark}",
                              print_msg=True)
