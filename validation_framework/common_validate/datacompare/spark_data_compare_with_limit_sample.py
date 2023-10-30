from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.datacompare.spark_data_compare import SparkDataCompare
from validation_framework.common_validate.utils import global_constants


class SparkDataCompareWithLimitSample(SparkDataCompare):

    def __init__(self,
                 passed_job_params: JobParam,
                 passed_common_operations: CommonOperations
                 ):
        passed_common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                               "__init__ method **********")
        self.table_1_sample_data = passed_job_params.table_1_endpoint.get_table_data()
        self.table_1_query = passed_job_params.get_params("table_1_query")
        validation_date = str(
            passed_job_params.get_params("validation_date")
        ).strip()
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
        passed_common_operations.log_and_print(f"Inside SparkDataCompareWithLimitSample, "
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

        table_1_distinct_pk_values = self.table_1_sample_data.selectExpr(
            f"CONCAT({pk_column_values_expr}) AS table_1_primary_key_column_value").distinct()
        table_1_distinct_pk_values_count = table_1_distinct_pk_values.count()

        passed_common_operations.log_and_print(f"Inside SparkDataCompareWithLimitSample, "
                                               f"__init__ method, "
                                               f"table_1_distinct_pk_values_count --> "
                                               f"{table_1_distinct_pk_values_count}")

        passed_common_operations.display_data_frame(table_1_distinct_pk_values,
                                                    "table_1_distinct_pk_values")
        table_1_distinct_pk_values.createOrReplaceTempView("table_1_distinct_pk_values")

        table_2_full_data = passed_job_params.table_2_endpoint.get_table_data()
        self.table_2_query = passed_job_params.get_params("table_2_query")
        table_2_full_data_counts = table_2_full_data.count()

        passed_common_operations.log_and_print(f"Inside SparkDataCompareWithLimitSample, "
                                               f"__init__ method, "
                                               f"table_2_full_data_counts --> "
                                               f"{table_2_full_data_counts}")

        table_2_full_data.createOrReplaceTempView("table_2_full_data")

        sql_query = f"SELECT A.* " \
                    f"FROM " \
                    f"(" \
                    f"SELECT *, " \
                    f"{final_pk_column_values_expr} " \
                    f"FROM " \
                    f"table_2_full_data" \
                    f") AS A " \
                    f"INNER JOIN " \
                    f"table_1_distinct_pk_values AS B " \
                    f"ON A.primary_key_column_value = " \
                    f"B.table_1_primary_key_column_value"

        passed_common_operations.log_and_print(f"Inside SparkDataCompareWithLimitSample, "
                                               f"__init__ method, "
                                               f"sql_query --> "
                                               f"{sql_query}")

        table_2_filtered_data = passed_job_params.spark_session.sql(sql_query)
        table_2_filtered_data_count = table_2_filtered_data.count()

        passed_common_operations.log_and_print(f"Inside SparkDataCompareWithLimitSample, "
                                               f"__init__ method, "
                                               f"table_2_filtered_data_count --> "
                                               f"{table_2_filtered_data_count}")

        passed_common_operations.display_data_frame(table_2_filtered_data,
                                                    "table_2_filtered_data")

        self.table_2_sample_data = table_2_filtered_data.drop("primary_key_column_value")
        table_2_sample_data_count = self.table_2_sample_data.count()

        passed_common_operations.log_and_print(f"Inside SparkDataCompareWithLimitSample, "
                                               f"__init__ method, "
                                               f"table_2_sample_data_count --> "
                                               f"{table_2_sample_data_count}")

        passed_common_operations.display_data_frame(self.table_2_sample_data,
                                                    "table_2_sample_data")

        table_1_keys = validation_key_name_keys.get(
            "TABLE_1_DETAILS", {})
        table_2_keys = validation_key_name_keys.get(
            "TABLE_2_DETAILS", {})

        table_1_full_name = table_1_keys.get("FULL_TABLE_NAME", "")
        table_2_full_name = table_2_keys.get("FULL_TABLE_NAME", "")
        where_clause = validation_key_name_keys.get("WHERE_CLAUSE", "").replace(
            "validation_date",
            validation_date
        )
        if where_clause:
            where_clause_1 = where_clause_2 = where_clause
        else:
            where_clause_1 = table_1_keys.get("WHERE_CLAUSE", "").replace(
                "validation_date",
                validation_date
            )
            where_clause_2 = table_2_keys.get("WHERE_CLAUSE", "").replace(
                "validation_date",
                validation_date
            )
        run_for_whole_table = validation_key_name_keys.get("RUN_FOR_WHOLE_TABLE", "")

        if run_for_whole_table == "True":
            check_count_query = f"SELECT " \
                                f"count(*) as count_of_records " \
                                f"FROM " \
                                f"`table_name` "
        else:
            check_count_query = f"SELECT " \
                                f"count(*) as count_of_records " \
                                f"FROM " \
                                f"`table_name` " \
                                f"WHERE " \
                                f"where_clause "

        self.table_1_records_count = int(passed_job_params.table_1_endpoint.execute_query_and_get_counts(
            check_count_query.replace(
                "table_name",
                table_1_full_name
            ).replace(
                "where_clause",
                where_clause_1
            ),
            "check_total_counts",
            "count_of_records"
        ))
        self.table_2_records_count = int(passed_job_params.table_2_endpoint.execute_query_and_get_counts(
            check_count_query.replace(
                "table_name",
                table_2_full_name
            ).replace(
                "where_clause",
                where_clause_2
            ),
            "check_total_counts",
            "count_of_records"
        ))
        if run_for_whole_table == "True":
            check_duplicates_pk_query = "SELECT " \
                                        f"count(distinct CONCAT({pk_column_values_expr})) " \
                                        f"as distinct_pk_counts " \
                                        f"FROM " \
                                        f"`table_name` "
        else:
            check_duplicates_pk_query = "SELECT " \
                                        f"count(distinct CONCAT({pk_column_values_expr})) " \
                                        f"as distinct_pk_counts " \
                                        f"FROM " \
                                        f"`table_name` " \
                                        f"WHERE " \
                                        f"where_clause "
        self.table_1_distinct_pk_count = int(passed_job_params.table_1_endpoint.execute_query_and_get_counts(
            check_duplicates_pk_query.replace(
                "table_name",
                table_1_full_name
            ).replace(
                "where_clause",
                where_clause_1
            ),
            "check_distinct_pk_count",
            "distinct_pk_counts"
        ))
        self.table_2_distinct_pk_count = int(passed_job_params.table_2_endpoint.execute_query_and_get_counts(
            check_duplicates_pk_query.replace(
                "table_name",
                table_2_full_name
            ).replace(
                "where_clause",
                where_clause_2
            ),
            "check_distinct_pk_count",
            "distinct_pk_counts"
        ))

        super().__init__(passed_job_params, passed_common_operations)

    def convert_to_tuple(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "convert_to_tuple method **********")
        return super().convert_to_tuple()

    def create_select_statement(self, column_name):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "create_select_statement method **********")
        return super().create_select_statement(column_name)

    def create_case_statement(self,
                              passed_column_name,
                              data_type_of_passed_column_in_base_df,
                              data_type_of_passed_column_in_compare_df):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "create_case_statement method **********")
        return super().create_case_statement(passed_column_name,
                                             data_type_of_passed_column_in_base_df,
                                             data_type_of_passed_column_in_compare_df)

    def generate_select_statement(self, passed_match_data=True):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "generate_select_statement method **********")
        return super().generate_select_statement(passed_match_data)

    def convert_row_missing_dataframe_to_desired_patter(self, passed_dataframe,
                                                        available_in, missing_in,
                                                        passed_name_of_dataframe):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "convert_row_missing_dataframe_to_desired_patter "
                                             "method **********")
        super().convert_row_missing_dataframe_to_desired_patter(passed_dataframe,
                                                                available_in, missing_in,
                                                                passed_name_of_dataframe)

    def get_sample_data_row_missing(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "get_sample_data_row_missing method **********")
        super().get_sample_data_row_missing()

    def get_sample_data_column_mismatch(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "get_sample_data_column_mismatch method **********")
        super().get_sample_data_column_mismatch()

    def form_summary_dict(self, passed_attribute_name, passed_attribute_value):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "form_summary_dict method **********")
        super().form_summary_dict(passed_attribute_name, passed_attribute_value)

    def get_sample_data_summary(self, summary_only=False):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "get_sample_data_summary method **********")
        super().get_sample_data_summary(summary_only)

    def run_sample_data_compare(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompareWithLimitSample "
                                             "run_sample_data_compare method **********")
        super().run_sample_data_compare()
