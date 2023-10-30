from abc import abstractmethod

from validation_framework.common_validate.validate.abstract_data_validator import ADataValidator
from validation_framework.common_validate.jobparams.job_param import JobParam


class exampleDataValidator(ADataValidator):

    def __init__(self, job_params: JobParam):
        print("********** Inside exampleDataValidator's __init__ method ********** ")
        super().__init__(job_params)
        self.common_operations. \
            log_and_print("********** Inside exampleDataValidator's "
                          "__init__ method ********** ", print_msg=True)

    def pre_load(self):
        self.common_operations. \
            log_and_print("********** Inside exampleDataValidator's "
                          "pre_load method ********** ", print_msg=True)
        super().pre_load()
        self.common_operations. \
            log_and_print("********** Inside exampleDataValidator's "
                          "pre_load completed ********** ", print_msg=True)

    def load(self):
        self.common_operations. \
            log_and_print("********** Inside exampleDataValidator's "
                          "load method ********** ", print_msg=True)
        super().load()
        self.common_operations. \
            log_and_print("********** Inside exampleDataValidator's "
                          "load completed ********** ", print_msg=True)

    def post_load(self):
        self.common_operations. \
            log_and_print("********** Inside exampleDataValidator's "
                          "post_load method ********** ", print_msg=True)
        super().post_load()
        self.common_operations. \
            log_and_print("********** Inside exampleDataValidator's "
                          "post_load completed ********** ", print_msg=True)
