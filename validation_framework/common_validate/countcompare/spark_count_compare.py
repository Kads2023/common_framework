from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.countcompare.count_compare import CountCompare

from validation_framework.common_validate.utils import global_constants


class SparkCountCompare(CountCompare):
    table_1_count_of_records = 0
    table_2_count_of_records = 0
    table_1_pk_counts = 0
    table_2_pk_counts = 0
    table_1_distinct_pk_count = 0
    table_2_distinct_pk_count = 0
    table_1_pk_null_counts = 0
    table_2_pk_null_counts = 0
    null_counts_dict = {}
    date_time_column_null_counts_dict = {}
    queries_executed = {}
    generic_counts_calculated = {}
    count_summary_dict_list = []

    def __init__(self,
                 passed_job_params: JobParam,
                 passed_common_operations: CommonOperations
                 ):
        self.job_params = passed_job_params
        self.common_operations = passed_common_operations
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "__init__ method **********")

        self.results_type = self.job_params.get_params("results_type")

        self.now_base_validation_type_keys, self.now_validation_type_keys = self.\
            common_operations.get_validation_type_keys(self.results_type, "results")

        self.validation_key_name = str(
            self.job_params.get_params("validation_key_name")
        ).strip().lower()
        self.validation_date = str(
            self.job_params.get_params("validation_date")
        ).strip()

        self.validation_key_name_keys = self.job_params.get_params(self.validation_key_name)

        self.join_columns_list = self.validation_key_name_keys.get("JOIN_KEYS", [])
        self.comparison_criteria = self.validation_key_name_keys.get(
            "COMPARISON_CRITERIA", "").replace(
            "validation_date",
            self.validation_date
        )
        comparison_type_from_config = self.job_params.safe_get_params(
            global_constants.common_section + ",COMPARISON_TYPE"
        )
        comparison_type_from_validation_key = self.validation_key_name_keys.get("COMPARISON_TYPE", "")
        if not comparison_type_from_validation_key:
            if not comparison_type_from_config:
                self.comparison_type = global_constants.default_comparison_type
            else:
                self.comparison_type = comparison_type_from_config
        else:
            self.comparison_type = comparison_type_from_validation_key
        self.date_type_group_by_columns_list = \
            self.validation_key_name_keys.get("DATE_TYPE_GROUP_BY_KEYS", [])
        self.other_group_by_columns_list = \
            self.validation_key_name_keys.get("OTHER_GROUP_BY_KEYS", [])
        self.date_time_columns_list = self.validation_key_name_keys.get("DATE_TIME_COLUMNS", [])
        self.type_code_columns_list = self.validation_key_name_keys.get("TYPE_CODE_COLUMNS", [])
        self.amount_columns_list = self.validation_key_name_keys.get("AMOUNT_COLUMNS", [])
        self.check_null_columns_list = self.validation_key_name_keys.get("CHECK_NULL_COLUMNS", [])

        self.column_mappings_dict = self.validation_key_name_keys.get("COLUMN_MAPPINGS", {})

        self.common_operations.log_and_print(f"Inside SparkCountCompare, "
                                             f"__init__ method, "
                                             f"join_columns_list --> "
                                             f"{self.join_columns_list}, "
                                             f"comparison_criteria --> "
                                             f"{self.comparison_criteria}, "
                                             f"comparison_type_from_config --> "
                                             f"{comparison_type_from_config}, "
                                             f"comparison_type_from_validation_key --> "
                                             f"{comparison_type_from_validation_key}, "
                                             f"default_comparison_type --> "
                                             f"{global_constants.default_comparison_type}, "
                                             f"comparison_type --> "
                                             f"{self.comparison_type}, "
                                             f"date_type_group_by_columns_list --> "
                                             f"{self.date_type_group_by_columns_list}, "
                                             f"other_group_by_columns_list --> "
                                             f"{self.other_group_by_columns_list}, "
                                             f"date_time_columns_list --> "
                                             f"{self.date_time_columns_list}, "
                                             f"type_code_columns_list --> "
                                             f"{self.type_code_columns_list}, "
                                             f"amount_columns_list --> "
                                             f"{self.amount_columns_list}, "
                                             f"check_null_columns_list --> "
                                             f"{self.check_null_columns_list}")

        self.table_1_keys = self.validation_key_name_keys.get(
            "TABLE_1_DETAILS", {})
        self.table_2_keys = self.validation_key_name_keys.get(
            "TABLE_2_DETAILS", {})

        self.table_1_full_name = \
            self.table_1_keys.get("FULL_TABLE_NAME", "")
        self.table_2_full_name = \
            self.table_2_keys.get("FULL_TABLE_NAME", "")

        if len(self.date_time_columns_list) == 0:
            self.table_1_date_time_columns_list = self.table_1_keys.get("DATE_TIME_COLUMNS", [])
            self.table_2_date_time_columns_list = self.table_2_keys.get("DATE_TIME_COLUMNS", [])
            self.date_time_columns_list = list(
                set(
                    self.table_1_date_time_columns_list +
                    self.table_2_date_time_columns_list
                )
            )
        self.where_clause_to_be_used_for_counts = \
            self.validation_key_name_keys.get("WHERE_CLAUSE_TO_BE_USED_FOR_COUNTS", "")
        self.where_clause = self.validation_key_name_keys.get("WHERE_CLAUSE", "").replace(
            "validation_date",
            self.validation_date
        )
        if self.where_clause:
            self.where_clause_1 = self.where_clause_2 = self.where_clause
        else:
            self.where_clause_1 = self.table_1_keys.get("WHERE_CLAUSE", "").replace(
                "validation_date",
                self.validation_date
            )
            self.where_clause_2 = self.table_2_keys.get("WHERE_CLAUSE", "").replace(
                "validation_date",
                self.validation_date
            )
        self.pk_column_values_expr = ""
        self.pk_null_values_check = ""
        for each_join_column in self.join_columns_list:
            self.pk_column_values_expr = self.common_operations.append_string(
                self.pk_column_values_expr,
                f"COALESCE(CAST(`{each_join_column}` AS STRING), '')",
                delimiter=global_constants.prinmary_key_concatenator
            )
            self.pk_null_values_check = self.common_operations.append_string(
                self.pk_null_values_check,
                f"`{each_join_column}` IS NULL",
                delimiter=" AND "
            )

        self.final_pk_column_values_expr = f"CONCAT({self.pk_column_values_expr}) " \
                                           f"AS primary_key_column_value"
        self.common_operations.log_and_print(f"Inside SparkCountCompare, "
                                             f"__init__ method, "
                                             f"final_pk_column_values_expr --> "
                                             f"{self.final_pk_column_values_expr} ")

        self.final_pk_column_names_value = ",".join(self.join_columns_list)
        self.spark = self.job_params.spark_session

        counter = 1
        self.group_by_date_columns_inner_select_list = []
        self.group_by_date_columns_outer_select_list = []
        for each_group_by_date_column in self.date_type_group_by_columns_list:
            self.group_by_date_columns_inner_select_list.append(
                f"COALESCE(CAST(DATE(`{each_group_by_date_column}`) AS STRING), '') AS date_value_{counter}")
            self.group_by_date_columns_outer_select_list.append(f"date_value_{counter}")
            counter += 1
        self.common_operations.log_and_print(f"Inside SparkCountCompare, "
                                             f"__init__ method, "
                                             f"group_by_date_columns_inner_select_list --> "
                                             f"{self.group_by_date_columns_inner_select_list}, "
                                             f"group_by_date_columns_outer_select_list --> "
                                             f"{self.group_by_date_columns_outer_select_list}")
        counter = 1
        self.group_by_other_columns_inner_select_list = []
        self.group_by_other_columns_outer_select_list = []
        for each_group_by_other_column in self.other_group_by_columns_list:
            self.group_by_other_columns_inner_select_list.append(
                f"`{each_group_by_other_column}` AS other_value_{counter}")
            self.group_by_other_columns_outer_select_list.append(f"other_value_{counter}")
            counter += 1
        self.common_operations.log_and_print(f"Inside SparkCountCompare, "
                                             f"__init__ method, "
                                             f"group_by_other_columns_inner_select_list --> "
                                             f"{self.group_by_other_columns_inner_select_list}, "
                                             f"group_by_other_columns_outer_select_list --> "
                                             f"{self.group_by_other_columns_outer_select_list}")

        self.group_by_all_columns_inner_select_list = []
        self.group_by_all_columns_outer_select_list = []

        self.group_by_all_columns_inner_select_list.extend(self.group_by_date_columns_inner_select_list)
        self.group_by_all_columns_inner_select_list.extend(self.group_by_other_columns_inner_select_list)

        self.group_by_all_columns_outer_select_list.extend(self.group_by_date_columns_outer_select_list)
        self.group_by_all_columns_outer_select_list.extend(self.group_by_other_columns_outer_select_list)

        self.common_operations.log_and_print(f"Inside SparkCountCompare, "
                                             f"__init__ method, "
                                             f"group_by_all_columns_inner_select_list --> "
                                             f"{self.group_by_all_columns_inner_select_list}, "
                                             f"group_by_all_columns_outer_select_list --> "
                                             f"{self.group_by_all_columns_outer_select_list}")
        self.group_by_columns_list = []
        self.group_by_all_columns_inner_select = ""
        self.group_by_all_columns_outer_select = ""
        self.group_by_column_names = ""
        self.group_by_column_values = ""
        if len(self.group_by_all_columns_inner_select_list) != 0 and \
                len(self.group_by_all_columns_outer_select_list) != 0:
            self.group_by_all_columns_inner_select = ', '.join(self.group_by_all_columns_inner_select_list)
            self.group_by_all_columns_outer_select = ', '.join(self.group_by_all_columns_outer_select_list)
            now_group_by_column_values = ""
            for each_column in self.group_by_all_columns_outer_select_list:
                now_group_by_column_values = self.common_operations.append_string(
                    now_group_by_column_values,
                    f"COALESCE(CAST({each_column} AS STRING), '')",
                    delimiter=", '|', ",
                    prefix="CONCAT("
                )
            if now_group_by_column_values:
                self.group_by_column_values = f"{now_group_by_column_values})"
            self.group_by_columns_list.extend(self.date_type_group_by_columns_list)
            self.group_by_columns_list.extend(self.other_group_by_columns_list)
            self.group_by_column_names = ', '.join(self.group_by_columns_list)
            self.common_operations.log_and_print(f"Inside SparkCountCompare, "
                                                 f"__init__ method, "
                                                 f"group_by_all_columns_inner_select --> "
                                                 f"{self.group_by_all_columns_inner_select}, "
                                                 f"group_by_all_columns_outer_select --> "
                                                 f"{self.group_by_all_columns_outer_select}, "
                                                 f"now_group_by_column_values --> "
                                                 f"{now_group_by_column_values}, "
                                                 f"group_by_column_values --> "
                                                 f"{self.group_by_column_values}")
        else:
            error_msg = f"Inside SparkCountCompare, " \
                        f"__init__ method, " \
                        f"len(group_by_all_columns_inner_select_list) --> " \
                        f"{len(self.group_by_all_columns_inner_select_list)}, " \
                        f"len(group_by_all_columns_outer_select_list) --> " \
                        f"{len(self.group_by_all_columns_outer_select_list)}"
            self.common_operations.log_and_print(error_msg)
        if self.where_clause_to_be_used_for_counts == "True":
            table_data_query = f"SELECT " \
                               f"* " \
                               f"FROM " \
                               f"`table_name` " \
                               f"WHERE " \
                               f"where_clause "
        else:
            table_data_query = f"SELECT " \
                               f"* " \
                               f"FROM " \
                               f"`table_name` "
        self.table_1_query = table_data_query.replace(
            "table_name",
            self.table_1_full_name
        ).replace(
            "where_clause",
            self.where_clause_1
        )
        self.table_2_query = table_data_query.replace(
            "table_name",
            self.table_2_full_name
        ).replace(
            "where_clause",
            self.where_clause_2
        )

    def check_total_counts(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "check_total_counts method **********")

    def check_null_counts(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "check_null_counts method **********")

    def check_pk_counts(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "check_pk_counts method **********")

    def check_distinct_pk_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "check_distinct_pk_count method **********")

    def check_pk_null_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "check_pk_null_count method **********")

    def check_date_time_column_null_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "check_date_time_column_null_count method **********")

    def generic_process_date_wise(self, passed_query, passed_column_name):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "generic_process_date_wise method **********")
        self.common_operations.log_and_print(f"Inside SparkCountCompare "
                                             f"generic_process_date_wise method, "
                                             f"passed_query --> {passed_query}, "
                                             f"passed_column_name --> {passed_column_name}")

    def check_group_by_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "check_group_by_count method **********")

    def check_type_code_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "check_type_code_count method **********")

    def check_amount_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "check_amount_count method **********")

    def form_count_summary_dict(self, passed_attribute_name, passed_attribute_value):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "form_count_summary_dict method **********")
        self.common_operations.log_and_print("Inside SparkCountCompare, "
                                             "form_count_summary_dict method,"
                                             "passed_attribute_name --> "
                                             "{passed_attribute_name}, "
                                             "passed_attribute_value --> "
                                             "{passed_attribute_value}")
        now_summary_dict = {
            "run_date_time": f"{global_constants.run_date_time}",
            "key_name": f"{self.job_params.validation_key_name}",
            "comparison_criteria": f"{self.comparison_criteria}",
            "comparison_type": f"{self.comparison_type}",
            "primary_key_columns": f"{self.final_pk_column_names_value}",
            "table_name_1": f"{self.table_1_full_name}",
            "table_name_2": f"{self.table_2_full_name}",
            "attribute_name": f"{passed_attribute_name}",
            "attribute_value": f"{passed_attribute_value}"
        }
        self.count_summary_dict_list.append(now_summary_dict)

    def check_summary(self):
        self.common_operations.log_and_print("Inside SparkCountCompare "
                                             "check_summary method, "
                                             "queries_executed --> "
                                             "{self.queries_executed}")
        self.common_operations.log_and_print("Inside SparkCountCompare "
                                             "check_summary method, "
                                             "generic_counts_calculated --> "
                                             f"{self.generic_counts_calculated}")
        for each_key, each_value in self.generic_counts_calculated.items():
            self.form_count_summary_dict(each_key, each_value)

        self.form_count_summary_dict("table_1_query",
                                     f"{self.table_1_query}")
        self.form_count_summary_dict("table_2_query",
                                     f"{self.table_2_query}")

        df_count_summary = self.spark.createDataFrame(self.count_summary_dict_list)
        self.common_operations.display_data_frame(df_count_summary, "df_count_summary")
        count_summary_table_name = self.now_validation_type_keys.get(
            global_constants.count_summary_results_key_name)
        self.job_params.results_endpoint.save_results(
            df_count_summary, count_summary_table_name)

    def run_counts_compare(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompare "
                                             "run_counts_compare method **********")
        self.check_total_counts()
        self.check_null_counts()
        self.check_pk_counts()
        self.check_distinct_pk_count()
        self.check_pk_null_count()
        self.check_date_time_column_null_count()
        self.check_group_by_count()
        self.check_type_code_count()
        self.check_amount_count()
        self.check_summary()
