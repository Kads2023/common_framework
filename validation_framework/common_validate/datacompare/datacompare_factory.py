from validation_framework.common_validate.datacompare.spark_datacompy_compare import SparkDataCompyCompare
from validation_framework.common_validate.datacompare.spark_data_compare_without_limit_sample import SparkDataCompareWithoutLimitSample
from validation_framework.common_validate.datacompare.spark_data_compare_with_limit_sample import SparkDataCompareWithLimitSample
from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.utils.keymaker_utils import KeyMakerApiProxy

from validation_framework.common_validate.utils import global_constants


class DataCompareFactory:

    def __init__(self,
                 passed_job_params: JobParam,
                 passed_common_operations: CommonOperations,
                 passed_keymaker: KeyMakerApiProxy
                 ):
        self.__job_params = passed_job_params
        self.__common_operations = passed_common_operations
        self.__keymaker = passed_keymaker
        self.__using_spark = self.__job_params.get_params("using_spark")
        self.__using_data_compy = self.__job_params.safe_get_params(
            global_constants.common_section + ",USING_DATA_COMPY"
        )
        self.__limit_sample_data = self.__job_params.safe_get_params(
            global_constants.common_section + ",LIMIT_SAMPLE_DATA"
        )

    def get_data_compare(self):
        self.__common_operations.\
            log_and_print("********** In get_data_compare of DataCompareFactory ********** ")
        if self.__using_data_compy == "True" and self.__using_spark == "True":
            self.__common_operations.\
                log_and_print("********** In get_data_compare of DataCompareFactory, "
                              "getting SparkDataCompyCompare class  ********** ")
            return SparkDataCompyCompare(
                self.__job_params,
                self.__common_operations
            )
        elif self.__using_spark == "True":
            self.__common_operations.\
                log_and_print("********** In get_data_compare of DataCompareFactory, "
                              f"using_spark --> {self.__using_spark}, "
                              f"limit_sample_data --> "
                              f"{self.__limit_sample_data} ********** ")
            if self.__limit_sample_data == "True":
                self.__common_operations. \
                    log_and_print(f"********** In get_data_compare of DataCompareFactory, "
                                  f"getting SparkDataCompareWithLimitSample ********** ")
                return SparkDataCompareWithLimitSample(
                    self.__job_params,
                    self.__common_operations
                )
            else:
                self.__common_operations. \
                    log_and_print(f"********** In get_data_compare of DataCompareFactory, "
                                  f"getting SparkDataCompareWithoutLimitSample ********** ")
                return SparkDataCompareWithoutLimitSample(
                    self.__job_params,
                    self.__common_operations
                )
        else:
            self.__common_operations.\
                log_and_print("we do not have the DataCompare implementation, "
                              f"using_spark --> {self.__using_spark}, "
                              f"using_data_compy --> {self.__using_data_compy}, "
                              f"", print_msg=True)
