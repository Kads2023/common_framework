import os

from validation_framework.common_validate.driver.base_dp_driver import BaseDpDriver
from validation_framework.common_validate.objects.input_args import InputArgs
from validation_framework.example_validate.driver.example_factory import exampleFactory


class exampleDpDriver(BaseDpDriver):

    def __init__(self, passed_input_params: InputArgs):
        print("********** Inside exampleDpDriver's __init__ method ********** ")
        super().__init__(passed_input_params)
        print(f"passed_input_params.tenant --> "
              f"{passed_input_params.tenant}, "
              f"passed_input_params.validation_key_name --> "
              f"{passed_input_params.validation_key_name}, "
              f"os.getenv('HOME')  --> {os.getenv('HOME')}")

        base_driver = exampleFactory()
        base_driver.start_load(passed_input_params)


if __name__ == '__main__':
    print("================================================================================")
    print("********** Inside main method ********** ")
    input_params = InputArgs()
    example = exampleDpDriver(input_params)
