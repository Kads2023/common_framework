from validation_framework.common_validate.driver.base_driver import BaseDriver
from validation_framework.common_validate.objects.input_args import InputArgs
from validation_framework.example_validate.driver.example_factory import exampleFactory


class exampleDriver(BaseDriver):

    def __init__(self, passed_input_params: InputArgs):
        print("********** Inside exampleDriver's __init__ method ********** ")
        base_driver = exampleFactory()
        base_driver.start_load(passed_input_params)


if __name__ == '__main__':
    print("================================================================================")
    print("********** Inside main method ********** ")
    input_params = InputArgs()
    print(f"input_params.tenant --> {input_params.tenant}, "
          f"input_params.validation_key_name --> "
          f"{input_params.validation_key_name}")
    example = exampleDriver(input_params)
