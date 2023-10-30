from validation_framework.common_validate.endpoints.bq_endpoint import BQEndpoint
from validation_framework.common_validate.endpoints.sf_endpoint import SFEndpoint
from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.utils.keymaker_utils import KeyMakerApiProxy


class EndpointFactory:

    def __init__(self,
                 passed_job_params: JobParam,
                 passed_common_operations: CommonOperations,
                 passed_keymaker: KeyMakerApiProxy):
        self.__job_params = passed_job_params
        self.__common_operations = passed_common_operations
        self.__keymaker = passed_keymaker

    def get_endpoint_connection(self, passed_endpoint_name: str, passed_endpoint_type: str):
        self.__common_operations.\
            log_and_print("********** In get_endpoint_connection of EndpointsFactory ********** ")
        final_endpoint_name = passed_endpoint_name.strip().upper()
        if final_endpoint_name == "BQ":
            self.__common_operations.\
                log_and_print("********** In getting BQ class  ********** ")
            return BQEndpoint(self.__job_params, self.__common_operations, self.__keymaker,
                              final_endpoint_name, passed_endpoint_type)
        elif final_endpoint_name == "SF":
            self.__common_operations.\
                log_and_print("********** In getting SF class  ********** ")
            return SFEndpoint(self.__job_params, self.__common_operations, self.__keymaker,
                              final_endpoint_name, passed_endpoint_type)
        else:
            self.__common_operations.\
                log_and_print("we do not have the Endpoint implementation, "
                              f"final_endpoint_type --> {final_endpoint_name}", print_msg=True)
