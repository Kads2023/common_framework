from abc import ABC, abstractmethod
from validation_framework.common_validate.validate.abstract_data_validator import ADataValidator
from validation_framework.common_validate.objects.input_args import InputArgs
from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.jobparams.jobparam_factory import JobParamFactory


class BaseDriver(ABC):

    @abstractmethod
    def get_loader(self, input_params: InputArgs, job_params: JobParam) -> ADataValidator:
        pass

    def start_load(self, input_params: InputArgs):
        print("********** Inside BaseDriver's start_load method ********** ")
        cur_job_params: JobParam = JobParamFactory(input_params).get_job_params()
        cur_loader: ADataValidator = self.get_loader(input_params, cur_job_params)
        cur_loader.__init__(cur_job_params)
        cur_loader.run_load()
