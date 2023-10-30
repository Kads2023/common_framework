from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.datacompare.spark_data_compare import SparkDataCompare
from validation_framework.common_validate.utils import global_constants


class SparkDataCompareWithoutLimitSample(SparkDataCompare):

    def __init__(self,
                 passed_job_params: JobParam,
                 passed_common_operations: CommonOperations
                 ):
        passed_common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                               "__init__ method **********")
        self.table_1_sample_data = passed_job_params.table_1_endpoint.get_table_data()
        self.table_1_query = passed_job_params.get_params("table_1_query")
        self.table_2_sample_data = passed_job_params.table_2_endpoint.get_table_data()
        self.table_2_query = passed_job_params.get_params("table_2_query")

        validation_key_name = str(
            passed_job_params.get_params("validation_key_name")
        ).strip().lower()
        validation_key_name_keys = passed_job_params.get_params(validation_key_name)
        join_columns_list = validation_key_name_keys.get("JOIN_KEYS", [])
        pk_column_values_expr = ""
        for each_join_column in join_columns_list:
            pk_column_values_expr = passed_common_operations.append_string(
                pk_column_values_expr,
                f"COALESCE(CAST(`{each_join_column}` AS STRING), '')",
                delimiter=global_constants.prinmary_key_concatenator
            )
        final_pk_column_values_expr = f"CONCAT({pk_column_values_expr}) " \
                                      f"AS primary_key_column_value"
        passed_common_operations.log_and_print(f"Inside SparkDataCompareWithoutLimitSample, "
                                               f"__init__ method, "
                                               f"validation_key_name --> "
                                               f"{validation_key_name}, "
                                               f"validation_key_name_keys --> "
                                               f"{validation_key_name_keys}, "
                                               f"join_columns_list --> "
                                               f"{join_columns_list}, "
                                               f"pk_column_values_expr --> "
                                               f"{pk_column_values_expr}, "
                                               f"final_pk_column_values_expr --> "
                                               f"{final_pk_column_values_expr}")

        self.table_1_records_count = self.table_1_sample_data.count()
        self.table_2_records_count = self.table_2_sample_data.count()
        self.table_1_distinct_pk_count = self.table_1_sample_data.selectExpr(
            f"CONCAT({pk_column_values_expr})").distinct().count()
        self.table_2_distinct_pk_count = self.table_2_sample_data.selectExpr(
            f"CONCAT({pk_column_values_expr})").distinct().count()

        super().__init__(passed_job_params, passed_common_operations)

    def convert_to_tuple(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "convert_to_tuple method **********")
        return super().convert_to_tuple()

    def create_select_statement(self, column_name):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "create_select_statement method **********")
        return super().create_select_statement(column_name)

    def create_case_statement(self,
                              passed_column_name,
                              data_type_of_passed_column_in_base_df,
                              data_type_of_passed_column_in_compare_df):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "create_case_statement method **********")
        return super().create_case_statement(passed_column_name,
                                             data_type_of_passed_column_in_base_df,
                                             data_type_of_passed_column_in_compare_df)

    def generate_select_statement(self, passed_match_data=True):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "generate_select_statement method **********")
        return super().generate_select_statement(passed_match_data)

    def convert_row_missing_dataframe_to_desired_patter(self, passed_dataframe,
                                                        available_in, missing_in,
                                                        passed_name_of_dataframe):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "convert_row_missing_dataframe_to_desired_patter "
                                             "method **********")
        super().convert_row_missing_dataframe_to_desired_patter(passed_dataframe,
                                                                available_in, missing_in,
                                                                passed_name_of_dataframe)

    def get_sample_data_row_missing(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "get_sample_data_row_missing method **********")
        super().get_sample_data_row_missing()

    def get_sample_data_column_mismatch(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "get_sample_data_column_mismatch method **********")
        super().get_sample_data_column_mismatch()

    def form_summary_dict(self, passed_attribute_name, passed_attribute_value):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "form_summary_dict method **********")
        super().form_summary_dict(passed_attribute_name, passed_attribute_value)

    def get_sample_data_summary(self, summary_only=False):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "get_sample_data_summary method **********")
        super().get_sample_data_summary(summary_only)

    def run_sample_data_compare(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithoutLimitSample "
                                             "run_sample_data_compare method **********")
        super().run_sample_data_compare()
