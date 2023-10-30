from validation_framework.common_validate.driver.base_driver import BaseDriver
from validation_framework.common_validate.validate.abstract_data_validator import ADataValidator
from validation_framework.example_validate.validate.example_data_validator import exampleDataValidator
from validation_framework.common_validate.objects.input_args import InputArgs
from validation_framework.common_validate.jobparams.job_param import JobParam


class exampleFactory(BaseDriver):

    def get_loader(self,
                   passed_input_params: InputArgs,
                   passed_job_params: JobParam) -> ADataValidator:
        print("********** Inside exampleFactory's get_loader method ********** ")
        passed_tenant = str(passed_input_params.tenant).strip().upper()
        if passed_tenant == "example":
            return exampleDataValidator(passed_job_params)
        else:
            print("we do not have the passed implementation, "
                  f"passed_tenant --> {passed_tenant}")
