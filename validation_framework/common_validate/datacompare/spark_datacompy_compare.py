from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.datacompare.data_compare import DataCompare

from validation_framework.common_validate.utils import global_constants

import datacompy
import json


class SparkDataCompyCompare(DataCompare):
    table_1_unq_rows_columns = list()
    table_2_unq_rows_columns = list()
    all_mismatch_columns = list()
    row_differences_dict_list = list()
    summary_dict_list = list()
    col_diff_dict_list = list()
    table_1_unq_rows_count = 0
    table_2_unq_rows_count = 0
    mismatch_rows_count = 0
    column_wise_mismatch_counts = {}
    matched_column = []
    mis_matched_column = []
    column_data_type_mismatch_dict = {}

    def __init__(self,
                 passed_job_params: JobParam,
                 passed_common_operations: CommonOperations
                 ):
        self.__job_params = passed_job_params
        self.__common_operations = passed_common_operations
        self.__common_operations.log_and_print("********** Inside SparkDataCompyCompare "
                                               "__init__ method **********")

        run_summary_only = self.__job_params.safe_get_params(
            global_constants.common_section + ",RUN_SUMMARY_ONLY"
        )
        self.__results_type = self.__job_params.get_params("results_type")

        self.__table_1_sample_data = self.__job_params.table_1_endpoint.get_table_data()
        self.__table_2_sample_data = self.__job_params.table_2_endpoint.get_table_data()

        self.__now_base_validation_type_keys, self.__now_validation_type_keys = self. \
            __common_operations.get_validation_type_keys(self.__results_type, "results")

        now_convert_column_names_to_lower_case = \
            self.__now_base_validation_type_keys.get("CONVERT_COLUMN_NAMES_TO_LOWER_CASE", "")
        if now_convert_column_names_to_lower_case:
            self.__convert_column_names_to_lower_case = now_convert_column_names_to_lower_case
        else:
            self.__convert_column_names_to_lower_case = global_constants.default_convert_column_names_to_lower_case

        self.__table_1_schema = self.__job_params.table_1_endpoint.get_table_schema(
            lower_column_names=self.__convert_column_names_to_lower_case)
        self.__table_1_columns = list(self.__table_1_schema.keys())

        self.__table_2_schema = self.__job_params.table_2_endpoint.get_table_schema(
            lower_column_names=self.__convert_column_names_to_lower_case)
        self.__table_2_columns = list(self.__table_2_schema.keys())

        self.__all_columns = set(
            self.__table_1_columns + self.__table_2_columns
        )
        self.__table_1_alone_columns = []
        self.__table_2_alone_columns = []
        self.__column_schema_issues = {}
        for each_col in self.__all_columns:
            avlbl_in_table_1 = False
            avlbl_in_table_2 = False
            if each_col in self.__table_1_columns:
                avlbl_in_table_1 = True
            if each_col in self.__table_2_columns:
                avlbl_in_table_2 = True
            each_col_dt_1 = str(self.__table_1_schema.get(each_col, "")).strip().upper()
            each_col_dt_2 = str(self.__table_2_schema.get(each_col, "")).strip().upper()
            if avlbl_in_table_1 and avlbl_in_table_2:
                if each_col_dt_1 != each_col_dt_2:
                    this_column_schema_issue = {
                        "TABLE_1_DATA_TYPE": f"{each_col_dt_1}",
                        "TABLE_2_DATA_TYPE": f"{each_col_dt_2}"
                    }
                    self.__column_schema_issues[each_col] = this_column_schema_issue
            elif avlbl_in_table_1 and not avlbl_in_table_2:
                self.__table_1_alone_columns.append(f"{each_col} --> {each_col_dt_1}")
            elif avlbl_in_table_2 and not avlbl_in_table_1:
                self.__table_2_alone_columns.append(f"{each_col} --> {each_col_dt_2}")

        self.__table_1_records_count = self.__table_1_sample_data.count()
        self.__table_2_records_count = self.__table_2_sample_data.count()

        self.__common_operations.log_and_print(f"Inside SparkDataCompyCompare, "
                                               f"__init__ method, "
                                               f"type(table_1_schema) --> {type(self.__table_1_schema)}, "
                                               f"table_1_schema --> {self.__table_1_schema}, "
                                               f"table_2_schema --> {self.__table_2_schema}, "
                                               f"table_1_records_count --> {self.__table_1_records_count}, "
                                               f"table_2_records_count --> {self.__table_2_records_count}")

        self.__validation_key_name = str(
            self.__job_params.get_params("validation_key_name")
        ).strip().lower()
        self.__validation_date = str(
            self.__job_params.get_params("validation_date")
        ).strip()

        self.__validation_key_name_keys = self.__job_params.get_params(self.__validation_key_name)

        self.__join_columns_list = self.__validation_key_name_keys.get("JOIN_KEYS", [])
        self.__comparison_criteria = self.__validation_key_name_keys.get(
            "COMPARISON_CRITERIA", "").replace(
            "validation_date",
            self.__validation_date
        )
        comparison_type_from_config = self.__job_params.safe_get_params(
            global_constants.common_section + ",COMPARISON_TYPE"
        )
        comparison_type_from_validation_key = self.__validation_key_name_keys.get("COMPARISON_TYPE", "")
        if not comparison_type_from_validation_key:
            if not comparison_type_from_config:
                self.__comparison_type = global_constants.default_comparison_type
            else:
                self.__comparison_type = comparison_type_from_config
        else:
            self.__comparison_type = comparison_type_from_validation_key
        self.__sample_record_count = int(self.__validation_key_name_keys.get(
            "SAMPLE_RECORD_COUNT", global_constants.default_sample_record_counts))
        self.__date_time_columns_list = self.__validation_key_name_keys.get("DATE_TIME_COLUMNS", [])
        self.__table_1_keys = self.__validation_key_name_keys.get(
            "TABLE_1_DETAILS", {})
        self.__table_2_keys = self.__validation_key_name_keys.get(
            "TABLE_2_DETAILS", {})

        self.__common_operations.log_and_print(f"Inside SparkDataCompyCompare, "
                                               f"__init__ method, "
                                               f"join_columns_list --> "
                                               f"{self.__join_columns_list}, "
                                               f"comparison_criteria --> "
                                               f"{self.__comparison_criteria}, "
                                               f"comparison_type --> "
                                               f"{self.__comparison_type}, "
                                               f"sample_record_count --> "
                                               f"{self.__sample_record_count}, "
                                               f"date_time_columns_list --> "
                                               f"{self.__date_time_columns_list}, "
                                               f"table_1_keys --> "
                                               f"{self.__table_1_keys}, "
                                               f"table_2_keys --> "
                                               f"{self.__table_2_keys}")

        self.__table_1_full_name = \
            self.__table_1_keys.get("FULL_TABLE_NAME", "")
        self.__table_2_full_name = \
            self.__table_2_keys.get("FULL_TABLE_NAME", "")

        if len(self.__date_time_columns_list) == 0:
            self.__table_1_date_time_columns_list = self.__table_1_keys.get("DATE_TIME_COLUMNS", [])
            self.__table_2_date_time_columns_list = self.__table_2_keys.get("DATE_TIME_COLUMNS", [])
            self.__date_time_columns_list = list(
                set(
                    self.__table_1_date_time_columns_list +
                    self.__table_2_date_time_columns_list
                )
            )

        self.__pk_column_values_expr = ""
        self.__column_schema_issues_keys = list(self.__column_schema_issues.keys())
        now_run_summary_only = ""
        for each_join_column in self.__join_columns_list:
            self.__pk_column_values_expr = self.__common_operations.append_string(
                self.__pk_column_values_expr,
                f"COALESCE(CAST(`{each_join_column}` AS STRING), '')",
                delimiter=global_constants.prinmary_key_concatenator
            )
            if each_join_column in self.__column_schema_issues_keys:
                now_run_summary_only = "True"
            if each_join_column not in self.__columns_in_both:
                now_run_summary_only = "True"

        if now_run_summary_only:
            self.__run_summary_only = now_run_summary_only
        else:
            self.__run_summary_only = run_summary_only

        self.__final_pk_column_values_expr = f"CONCAT({self.__pk_column_values_expr}) " \
                                             f"AS primary_key_column_value"
        self.__common_operations.log_and_print(f"Inside SparkDataCompyCompare, "
                                               f"__init__ method, "
                                               f"final_pk_column_values_expr --> "
                                               f"{self.__final_pk_column_values_expr} ")

        self.__final_pk_column_names_value = ",".join(self.__join_columns_list)

        self.__table_1_distinct_pk_count = self.__table_1_sample_data.selectExpr(
            f"CONCAT({self.__pk_column_values_expr})").distinct().count()
        self.__table_2_distinct_pk_count = self.__table_2_sample_data.selectExpr(
            f"CONCAT({self.__pk_column_values_expr})").distinct().count()

        self.__common_operations.log_and_print(f"Inside SparkDataCompyCompare, "
                                               f"__init__ method, "
                                               f"table_1_records_count --> {self.__table_1_records_count}, "
                                               f"table_2_records_count --> {self.__table_2_records_count}, "
                                               f"table_1_distinct_pk_count --> {self.__table_1_distinct_pk_count}, "
                                               f"table_2_distinct_pk_count --> {self.__table_2_distinct_pk_count}")

        self.__spark = self.__job_params.spark_session

        if self.__table_1_records_count > 0 \
                and self.__table_2_records_count > 0:
            if self.__run_summary_only == "False":
                self.__sample_data_compare = datacompy.SparkCompare(
                    self.__spark,
                    self.__table_1_sample_data,
                    self.__table_2_sample_data,
                    join_columns=self.__join_columns_list,
                    cache_intermediates=True,
                    show_all_columns=True
                )
                # self.__sample_data_compare.report()

                self.__columns_compared = list(self.__sample_data_compare.columns_in_both)
                self.__common_operations.log_and_print(f"Inside SparkDataCompyCompare, "
                                                       f"__init__ method, "
                                                       f"columns_compared --> {self.__columns_compared}")

                self.__other_columns = []
                for each_column in self.__columns_compared:
                    if each_column not in self.__join_columns_list:
                        if each_column in self.__date_time_columns_list:
                            self.__other_columns.append(
                                f"COALESCE(CAST(`{each_column}` AS STRING), '') AS `{each_column}`")
                        else:
                            self.__other_columns.append(f"`{each_column}`")

                self.run_sample_data_compare()
            else:
                self.get_sample_data_summary(summary_only=True)
        else:
            self.get_sample_data_summary(summary_only=True)
            self.__common_operations.log_and_print(
                "Inside SparkDataCompyCompare, "
                "__init__ method, "
                f"table_full_name_1 --> {self.__table_1_full_name}, "
                f"table_1_records_count --> {self.__table_1_records_count}, "
                f"table_full_name_2 --> {self.__table_2_full_name}, "
                f"table_2_records_count --> {self.__table_2_records_count}, "
                f"{self.__table_1_full_name} OR {self.__table_2_full_name}, "
                f"HAS ZERO RECORDS HENCE CANNOT DO DATA COMPARE",
                logger_type="error")

    def convert_row_missing_dataframe_to_desired_patter(self, passed_dataframe,
                                                        available_in, missing_in,
                                                        passed_name_of_dataframe):
        self.__common_operations.log_and_print("********** Inside SparkDataCompyCompare "
                                               "convert_row_missing_dataframe_to_desired_patter "
                                               "method **********")
        self.__common_operations.display_data_frame(passed_dataframe, passed_name_of_dataframe)

        unq_rows_fin = passed_dataframe.selectExpr(
            *self.__other_columns,
            f"'{global_constants.run_date_time}' AS run_date_time",
            f"'{self.__job_params.validation_key_name}' AS key_name",
            f"'{available_in}' AS available_in",
            f"'{missing_in}' AS missing_in",
            f"{self.__final_pk_column_values_expr}",
            f"'{self.__final_pk_column_names_value}' AS primary_key_column_names"
        ).distinct()

        self.__common_operations.display_data_frame(unq_rows_fin,
                                                    f"{passed_name_of_dataframe}_unq_rows_fin")

        results_json = unq_rows_fin.toJSON().map(lambda j: json.loads(j)).collect()
        for each_value in results_json:
            self.__common_operations.log_and_print(f"Inside SparkDataCompyCompare, "
                                                   f"convert_row_missing_dataframe_to_desired_patter "
                                                   f"method, each_value_df1 --> {each_value}")
            row_differences_dict = {
                "run_date_time": f'{each_value.pop("run_date_time", "")}',
                "key_name": f'{each_value.pop("key_name", "")}',
                "available_in": f'{each_value.pop("available_in", "")}',
                "missing_in": f'{each_value.pop("missing_in", "")}',
                "primary_key_column_names": f'{each_value.pop("primary_key_column_names", "")}',
                "primary_key_column_value": f'{each_value.pop("primary_key_column_value", "")}',
                "record_value": f"{each_value}"
            }
            self.__common_operations.log_and_print(f"Inside SparkDataCompyCompare, "
                                                   f"convert_row_missing_dataframe_to_desired_patter "
                                                   f"method, row_differences_dict --> {row_differences_dict}")
            self.row_differences_dict_list.append(row_differences_dict)

        self.__common_operations.log_and_print(
            "Inside SparkDataCompyCompare"
            "convert_row_missing_dataframe_to_desired_patter "
            "method, "
            f"len(row_differences_dict_list) --> {len(self.row_differences_dict_list)}, "
            f"row_differences_dict_list --> {self.row_differences_dict_list}")

    def get_sample_data_row_missing(self):
        self.__common_operations.log_and_print("********** Inside SparkDataCompyCompare "
                                               "get_sample_data_row_missing method **********")
        table_1_df_unq_rows = self.__sample_data_compare.rows_only_base
        self.table_1_unq_rows_count = table_1_df_unq_rows.count()
        self.table_1_unq_rows_columns = table_1_df_unq_rows.columns
        self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                               "get_sample_data_row_missing method, "
                                               "table_1_unq_rows_count --> "
                                               f"{self.table_1_unq_rows_count}, "
                                               f"table_1_unq_rows_columns --> "
                                               f"{self.table_1_unq_rows_columns}")
        if self.table_1_unq_rows_count > 0:
            self.convert_row_missing_dataframe_to_desired_patter(
                table_1_df_unq_rows.limit(self.__sample_record_count),
                f"{self.__table_1_full_name}",
                f"{self.__table_2_full_name}",
                "table_1_df_unq_rows"
            )

        table_2_df_unq_rows = self.__sample_data_compare.rows_only_compare
        self.table_2_unq_rows_count = table_2_df_unq_rows.count()
        self.table_2_unq_rows_columns = table_2_df_unq_rows.columns
        self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                               "get_sample_data_row_missing method, "
                                               "table_2_unq_rows_count --> "
                                               f"{self.table_2_unq_rows_count}, "
                                               f"table_2_unq_rows_columns --> "
                                               f"{self.table_2_unq_rows_columns}")
        if self.table_2_unq_rows_count > 0:
            self.convert_row_missing_dataframe_to_desired_patter(
                table_2_df_unq_rows.limit(self.__sample_record_count),
                f"{self.__table_2_full_name}",
                f"{self.__table_1_full_name}",
                "table_2_df_unq_rows"
            )

        self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                               "get_sample_data_row_missing method, "
                                               "len(row_differences_dict_list) --> "
                                               f"{len(self.row_differences_dict_list)}")
        if len(self.row_differences_dict_list) > 0:
            df_row_differences = self.__spark.createDataFrame(self.row_differences_dict_list)
            self.__common_operations.display_data_frame(df_row_differences, "df_row_differences")
            sample_data_row_missing_table_name = self.__now_validation_type_keys.get(
                global_constants.sample_data_row_missing_results_key_name)
            self.__job_params.results_endpoint.save_results(
                df_row_differences, sample_data_row_missing_table_name)

    def get_sample_data_column_mismatch(self):
        self.__common_operations.log_and_print("********** Inside SparkDataCompyCompare "
                                               "get_sample_data_column_mismatch method **********")
        df_mismatch_all = self.__sample_data_compare.rows_both_mismatch
        self.mismatch_rows_count = df_mismatch_all.count()
        self.all_mismatch_columns = df_mismatch_all.columns

        if len(df_mismatch_all.head(1)) != 0:
            self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                   "get_sample_data_column_mismatch method, "
                                                   "Rows that are not matched in both data, "
                                                   f"mismatch_rows_count --> {self.mismatch_rows_count}, "
                                                   f"all_mismatch_columns --> {self.all_mismatch_columns}")
            self.__common_operations.display_data_frame(df_mismatch_all, "df_mismatch_all")
            for column_name in self.__columns_compared:
                self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                       "get_sample_data_column_mismatch method, "
                                                       f"COMPARING column_name --> {column_name}")
                if f"{column_name}_match" in self.all_mismatch_columns:
                    self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                           "get_sample_data_column_mismatch method, "
                                                           f"COMPARING column_name --> "
                                                           f"{column_name}, "
                                                           f"{column_name}_match FOUND IN "
                                                           f"{self.all_mismatch_columns}")
                    col_df = df_mismatch_all.select(
                        *self.__join_columns_list,
                        f"A.{column_name}_base",
                        f"A.{column_name}_compare"
                    ).where(f"{column_name}_match == 'false'").distinct()
                    now_col_diff_count = col_df.count()
                    self.column_wise_mismatch_counts[column_name] = now_col_diff_count
                    self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                           "get_sample_data_column_mismatch method, "
                                                           f"COMPARING column_name --> "
                                                           f"{column_name}, "
                                                           f"{column_name}_match FOUND IN "
                                                           f"{self.all_mismatch_columns}, "
                                                           f"now_col_diff_count --> {now_col_diff_count}")
                    if len(col_df.head(1)) != 0:
                        self.mis_matched_column.append(column_name)
                        self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                               "get_sample_data_column_mismatch method, "
                                                               "\n ******* "
                                                               f"MISMATCH IN COLUMN --> "
                                                               f"'{column_name}' *********")
                        self.__common_operations.display_data_frame(col_df, f"col_df_{column_name}")
                        results_col_df = col_df.limit(self.__sample_record_count). \
                            toJSON().map(lambda j: json.loads(j)).collect()
                        self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                               "get_sample_data_column_mismatch method, "
                                                               "COMPARING column_name --> "
                                                               f"{column_name}, "
                                                               f"results_col_df --> "
                                                               f"{results_col_df}")
                        for each_col_value in results_col_df:
                            column_value_from_table_1 = each_col_value.pop(f"{column_name}_base", "")
                            column_value_from_table_2 = each_col_value.pop(f"{column_name}_compare", "")
                            column_data_type_from_table_1 = self.__table_1_schema.get(column_name, "")
                            column_data_type_from_table_2 = self.__table_2_schema.get(column_name, "")
                            self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                                   "get_sample_data_column_mismatch method, "
                                                                   f"column_value_from_table_1 --> "
                                                                   f"{column_value_from_table_1}, "
                                                                   f"column_value_from_table_2 --> "
                                                                   f"{column_value_from_table_2}, "
                                                                   f"column_data_type_from_table_1 --> "
                                                                   f"{column_data_type_from_table_1}, "
                                                                   f"column_data_type_from_table_2 --> "
                                                                   f"{column_data_type_from_table_2}")
                            col_diff_dict = {
                                "run_date_time": f"{global_constants.run_date_time}",
                                "key_name": f"{self.__job_params.validation_key_name}",
                                "column_name": f"{column_name}",
                                "primary_key_column_names": f"{self.__final_pk_column_names_value}",
                                "primary_key_column_value": f"{each_col_value}",
                                "table_name_1": f"{self.__table_1_full_name}",
                                "table_name_2": f"{self.__table_2_full_name}",
                                "column_data_type_from_table_1": f"{column_data_type_from_table_1}",
                                "column_data_type_from_table_2": f"{column_data_type_from_table_2}",
                                "column_value_from_table_1": f"{column_value_from_table_1}",
                                "column_value_from_table_2": f"{column_value_from_table_2}"
                            }
                            self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                                   "get_sample_data_column_mismatch method, "
                                                                   f"col_diff_dict --> {col_diff_dict}")
                            self.col_diff_dict_list.append(col_diff_dict)
                        self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                               "get_sample_data_column_mismatch method, "
                                                               f"len(col_diff_dict_list) --> "
                                                               f"{len(self.col_diff_dict_list)}, "
                                                               f"col_diff_dict_list --> "
                                                               f"{self.col_diff_dict_list}")
                    else:
                        self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                               "get_sample_data_column_mismatch method, "
                                                               f"\n ******* "
                                                               f"COLUMN '{column_name}' MATCH, "
                                                               f"NO ISSUES *********")
                        self.matched_column.append(column_name)
                else:
                    self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                           "get_sample_data_column_mismatch method, "
                                                           f"COMPARING column_name --> {column_name}, "
                                                           f"{column_name}_match, "
                                                           f"NOT FOUND IN {self.all_mismatch_columns}")
        else:
            self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                                   "get_sample_data_column_mismatch method, "
                                                   f"\n ******* "
                                                   f"COLUMNS ARE COMPLETELY MATCHING, "
                                                   f"NO ISSUES *********")

        self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                               "get_sample_data_column_mismatch method, "
                                               "len(col_diff_dict_list) --> "
                                               f"{len(self.col_diff_dict_list)}")
        if len(self.col_diff_dict_list) > 0:
            df_coll_differences = self.__spark.createDataFrame(self.col_diff_dict_list)
            self.__common_operations.display_data_frame(df_coll_differences, "df_coll_differences")
            sample_data_column_mismatch_table_name = self.__now_validation_type_keys.get(
                global_constants.sample_data_column_mismatch_results_key_name)
            self.__job_params.results_endpoint.save_results(
                df_coll_differences, sample_data_column_mismatch_table_name)

    def form_summary_dict(self, passed_attribute_name, passed_attribute_value):
        self.__common_operations.log_and_print("********** Inside SparkDataCompyCompare "
                                               "form_summary_dict method **********")
        self.__common_operations.log_and_print("Inside SparkDataCompyCompare, "
                                               "form_summary_dict method, "
                                               f"passed_attribute_name --> "
                                               f"{passed_attribute_name}, "
                                               f"passed_attribute_value --> "
                                               f"{passed_attribute_value}")

        now_summary_dict = {
            "run_date_time": f"{global_constants.run_date_time}",
            "key_name": f"{self.__job_params.validation_key_name}",
            "comparison_criteria": f"{self.__comparison_criteria}",
            "comparison_type": f"{self.__comparison_type}",
            "primary_key_columns": f"{self.__final_pk_column_names_value}",
            "table_name_1": f"{self.__table_1_full_name}",
            "table_name_2": f"{self.__table_2_full_name}",
            "attribute_name": f"{passed_attribute_name}",
            "attribute_value": f"{passed_attribute_value}"
        }
        self.summary_dict_list.append(now_summary_dict)

    def check_column_schema_issues(self):
        self.__common_operations.log_and_print("********** Inside SparkDataCompyCompare "
                                               "check_column_schema_issues method **********")
        table_1_columns = list(self.__table_1_schema.keys())
        table_2_columns = list(self.__table_2_schema.keys())

        all_columns = list(set(table_1_columns + table_2_columns))

        for each_column_name in all_columns:
            table_1_data_type = str(self.__table_1_schema.get(
                each_column_name, "NOT_AVAILABLE")).strip().lower()
            table_2_data_type = str(self.__table_2_schema.get(
                each_column_name, "NOT_AVAILABLE")).strip().lower()
            if table_1_data_type != table_2_data_type:
                self.column_data_type_mismatch_dict[each_column_name] = {
                    "table_1_data_type": f"{table_1_data_type}",
                    "table_2_data_type": f"{table_2_data_type}"
                }
        self.__common_operations.log_and_print(f"Inside SparkDataCompyCompare, "
                                               f"check_column_schema_issues method, "
                                               f"len(column_data_type_mismatch_dict) --> "
                                               f"{len(self.column_data_type_mismatch_dict)}, "
                                               f"column_data_type_mismatch_dict --> "
                                               f"{self.column_data_type_mismatch_dict}")

    def get_sample_data_summary(self, summary_only=False):
        self.__common_operations.log_and_print("********** Inside SparkDataCompyCompare "
                                               "get_sample_data_summary method **********")

        self.form_summary_dict("run_time_params", self.__job_params.args)

        self.form_summary_dict("records_count_in_table_1", self.__table_1_records_count)
        self.form_summary_dict("records_count_in_table_2", self.__table_2_records_count)

        self.form_summary_dict("distinct_primary_key_count_in_table_1",
                               self.__table_1_distinct_pk_count)
        self.form_summary_dict("distinct_primary_key_count_in_table_2",
                               self.__table_2_distinct_pk_count)
        self.form_summary_dict("columns_with_data_type_issues",
                               f"{self.__column_schema_issues}")
        self.form_summary_dict("columns_in_table_1_alone",
                               f"{self.__table_1_alone_columns}")
        self.form_summary_dict("columns_in_table_2_alone",
                               f"{self.__table_2_alone_columns}")

        if not summary_only:
            self.form_summary_dict("count_of_rows_missing_in_table_1", self.table_2_unq_rows_count)
            self.form_summary_dict("count_of_rows_missing_in_table_2", self.table_1_unq_rows_count)

            self.form_summary_dict("count_of_rows_with_mismatch", self.mismatch_rows_count)

            self.form_summary_dict("number_of_columns_in_table_1", len(self.__table_1_schema))
            self.form_summary_dict("number_of_columns_in_table_2", len(self.__table_2_schema))
            self.form_summary_dict("number_of_common_columns", len(self.__columns_compared))

            self.form_summary_dict("number_of_columns_in_table_1_not_in_table_2",
                                   len(self.__sample_data_compare.columns_only_base))
            self.form_summary_dict("number_of_columns_in_table_2_not_in_table_1",
                                   len(self.__sample_data_compare.columns_only_compare))

            self.form_summary_dict("number_of_columns_which_matched",
                                   len(self.matched_column))
            self.form_summary_dict("number_of_columns_which_have_mismatch",
                                   len(self.mis_matched_column))

            self.form_summary_dict("columns_with_data_type_issues",
                                   f"{self.column_data_type_mismatch_dict}")

            self.form_summary_dict("columns_which_matched",
                                   ', '.join(self.matched_column))
            self.form_summary_dict("columns_which_have_mismatch",
                                   ', '.join(self.mis_matched_column))

            self.form_summary_dict("columns_wise_count_of_records_with_mismatch",
                                   f"{self.column_wise_mismatch_counts}")

        df_summary = self.__spark.createDataFrame(self.summary_dict_list)
        self.__common_operations.display_data_frame(df_summary, "df_summary")
        sample_data_summary_table_name = self.__now_validation_type_keys.get(
            global_constants.sample_data_summary_results_key_name)
        self.__job_params.results_endpoint.save_results(
            df_summary, sample_data_summary_table_name)

    def run_sample_data_compare(self):
        self.__common_operations.log_and_print("********** Inside SparkDataCompyCompare "
                                               "run_sample_data_compare method **********")
        self.get_sample_data_row_missing()
        self.get_sample_data_column_mismatch()
        self.check_column_schema_issues()
        self.get_sample_data_summary()
