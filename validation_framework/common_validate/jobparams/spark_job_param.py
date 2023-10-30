from validation_framework.common_validate.objects.input_args import InputArgs
from validation_framework.common_validate.jobparams.job_param import JobParam


class SparkJobParam(JobParam):
    print("********** Inside SparkJobParam ********** ")

    def __init__(self, input_args: InputArgs):
        print("********** Inside SparkJobParam's __init__ method ********** ")
        super().__init__(input_args)
        print(f'Inside SparkJobParam __init__ method, '
              f'input_args.args --> {input_args.args}')
        from pyspark.sql import SparkSession
        self.spark_session = SparkSession.builder.appName('validate').getOrCreate()
        print(f'Inside SparkJobParam __init__ method,'
              f'self.args --> {self.get_params("args")}, '
              f'self.tenant --> {self.get_params("tenant")}, '
              f'self.config_dir --> {self.get_params("config_dir")}, '
              f'self.validation_type --> {self.get_params("validation_type")}, '
              f'self.validation_sub_type --> {self.get_params("validation_sub_type")}, '
              f'self.user_dir --> {self.get_params("user_dir")}, '
              f'self.env --> {self.env}, '
              f'self.home --> {self.home}')

    def set_common_config(self):
        print("********** Inside SparkJobParam's set_common_config method ********** ")
        super().set_common_config()

    def check_table_keys(self):
        print("********** Inside SparkJobParam's check_table_keys method ********** ")
        super().check_table_keys()

    def set_table_keys(self):
        print("********** Inside SparkJobParam's set_table_keys method ********** ")
        super().set_table_keys()

    def override_params_from_config(self):
        print("********** Inside SparkJobParam's override_params_from_config method ********** ")
        super().override_params_from_config()

    def set_params(self, passed_key_name: str, passed_value: str):
        print("********** Inside SparkJobParam's set_params method ********** ")
        super().set_params(passed_key_name, passed_value)

    def safe_get_params(self, func_key_name: str, default_value=""):
        print("********** Inside SparkJobParam's safe_get_params method ********** ")
        return super().safe_get_params(func_key_name, default_value)

    def get_params(self, passed_key_name: str, default_value=""):
        print("********** Inside SparkJobParam's safe_get_params method ********** ")
        return super().get_params(passed_key_name, default_value)
