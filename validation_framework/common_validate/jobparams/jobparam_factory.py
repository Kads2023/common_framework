from validation_framework.common_validate.jobparams.spark_job_param import SparkJobParam
from validation_framework.common_validate.jobparams.python_job_param import PythonJobParam
from validation_framework.common_validate.objects.input_args import InputArgs


class JobParamFactory:
    def __init__(self,
                 passed_input_args: InputArgs):
        self.__input_args = passed_input_args

    def get_job_params(self):
        print("********** In get_job_params of JobParamFactory ********** ")
        final_using_spark = self.__input_args.using_spark.strip().capitalize()
        print(f"In get_job_params of JobParamFactory, "
              f"using_spark --> {final_using_spark}")
        if final_using_spark == "True":
            print("********** In getting Spark Job Params class  ********** ")
            return SparkJobParam(self.__input_args)
        elif final_using_spark == "False":
            print("********** In getting Python Job Params class  ********** ")
            return PythonJobParam(self.__input_args)
        else:
            print("we do not have the Job Params implementation, "
                  f"using_spark --> {final_using_spark}")
