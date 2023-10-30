from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.datacompare.data_compare import DataCompare

from validation_framework.common_validate.utils import global_constants

import json

from enum import Enum
from itertools import chain

try:
    from pyspark.sql import functions as f
except ImportError:
    pass


class MatchType(Enum):
    MISMATCH, MATCH, KNOWN_DIFFERENCE = range(3)


def decimal_comparator():
    class DecimalComparator(str):
        def __eq__(self, other):
            return len(other) >= 7 and other[0:7] == "decimal"

    return DecimalComparator("decimal")


NUMERIC_SPARK_TYPES = [
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    decimal_comparator(),
]


def _is_comparable(type1, type2):
    """Checks if two Spark data types can be safely compared.
    Two data types are considered comparable if any of the following apply:
        1. Both data types are the same
        2. Both data types are numeric

    Parameters
    ----------
    type1 : str
        A string representation of a Spark data type
    type2 : str
        A string representation of a Spark data type

    Returns
    -------
    bool
        True if both data types are comparable
    """
    return type1 == type2 or (type1 in NUMERIC_SPARK_TYPES and type2 in NUMERIC_SPARK_TYPES)


class SparkDataCompare(DataCompare):
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
    known_differences = None
    rel_tol = 0
    abs_tol = 0
    table_1_sample_data = None
    table_2_sample_data = None
    table_1_query = ""
    table_2_query = ""

    def __init__(self,
                 passed_job_params: JobParam,
                 passed_common_operations: CommonOperations
                 ):
        self.job_params = passed_job_params
        self.common_operations = passed_common_operations
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "__init__ method **********")

        run_summary_only = self.job_params.safe_get_params(
            global_constants.common_section + ",RUN_SUMMARY_ONLY"
        )
        self.limit_sample_data = self.job_params.safe_get_params(
            global_constants.common_section + ",LIMIT_SAMPLE_DATA"
        )
        self.results_type = self.job_params.get_params("results_type")
        self.table_1_sample_data.createOrReplaceTempView("base_table")
        self.base_table_schema = self.common_operations.list_of_tuples_to_dict(
            self.table_1_sample_data.dtypes
        )
        self.table_2_sample_data.createOrReplaceTempView("compare_table")
        self.compare_table_schema = self.common_operations.list_of_tuples_to_dict(
            self.table_2_sample_data.dtypes
        )
        self.now_base_validation_type_keys, self.now_validation_type_keys = self.\
            common_operations.get_validation_type_keys(self.results_type, "results")

        now_convert_column_names_to_lower_case = \
            self.now_base_validation_type_keys.get("CONVERT_COLUMN_NAMES_TO_LOWER_CASE", "")
        if now_convert_column_names_to_lower_case:
            self.convert_column_names_to_lower_case = now_convert_column_names_to_lower_case
        else:
            self.convert_column_names_to_lower_case = global_constants.default_convert_column_names_to_lower_case

        self.table_1_schema = self.job_params.table_1_endpoint.get_table_schema(
            lower_column_names=self.convert_column_names_to_lower_case)
        self.table_1_columns = list(self.table_1_schema.keys())

        self.table_2_schema = self.job_params.table_2_endpoint.get_table_schema(
            lower_column_names=self.convert_column_names_to_lower_case)
        self.table_2_columns = list(self.table_2_schema.keys())

        self.all_columns = set(
            self.table_1_columns + self.table_2_columns
        )

        self.validation_type = str(
            self.job_params.get_params("validation_type")
        ).strip().upper()
        self.validation_sub_type = str(
            self.job_params.safe_get_params("validation_sub_type")
        ).strip().upper()

        validation_type_keys = self.job_params.get_params(
            self.validation_type
        )

        if self.validation_sub_type:
            now_validation_sub_type_keys = self.job_params.safe_get_params(
                self.validation_type + "," +
                self.validation_sub_type
            ) or {}
            if now_validation_sub_type_keys:
                self.validation_type_keys = now_validation_sub_type_keys
            else:
                self.validation_type_keys = validation_type_keys
        else:
            self.validation_type_keys = validation_type_keys

        validation_type_keys_data = self.validation_type_keys.get("DATA", {})
        datatype_mapping = validation_type_keys_data.get("DATATYPE_MAPPING", {})
        datatype_mapping_keys = list(datatype_mapping.keys())

        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"__init__ method, "
                                             f"validation_type --> "
                                             f"{self.validation_type}, "
                                             f"validation_sub_type --> "
                                             f"{self.validation_sub_type}, "
                                             f"validation_type_keys --> "
                                             f"{validation_type_keys}, "
                                             f"final_validation_type_keys --> "
                                             f"{self.validation_type_keys}, "
                                             f"validation_type_keys_data --> "
                                             f"{validation_type_keys_data}, "
                                             f"datatype_mapping --> {datatype_mapping}")

        self.validation_key_name = str(
            self.job_params.get_params("validation_key_name")
        ).strip().lower()
        self.validation_date = str(
            self.job_params.get_params("validation_date")
        ).strip()

        self.validation_key_name_keys = self.job_params.get_params(self.validation_key_name)

        allow_datatype_mapping_config = self.validation_type_keys.get("ALLOW_DATATYPE_MAPPING", "")
        allow_datatype_mapping_keys = self.validation_key_name_keys.get("ALLOW_DATATYPE_MAPPING", "")

        if allow_datatype_mapping_keys:
            self.allow_datatype_mapping = allow_datatype_mapping_keys
        elif allow_datatype_mapping_config:
            self.allow_datatype_mapping = allow_datatype_mapping_config
        else:
            self.allow_datatype_mapping = global_constants.default_allow_datatype_mapping

        self.table_1_alone_columns = []
        self.table_1_alone_columns_details = []
        self.table_2_alone_columns = []
        self.table_2_alone_columns_details = []
        self.column_schema_issues = {}
        self.columns_in_both = []
        for each_col in self.all_columns:
            column_schema_issue = False
            available_in_table_1 = False
            available_in_table_2 = False
            if each_col in self.table_1_columns:
                available_in_table_1 = True
            if each_col in self.table_2_columns:
                available_in_table_2 = True
            each_col_dt_1 = str(self.table_1_schema.get(each_col, "")).strip().upper()
            each_col_dt_2 = str(self.table_2_schema.get(each_col, "")).strip().upper()
            if available_in_table_1 and available_in_table_2:
                if each_col_dt_1 != each_col_dt_2:
                    column_schema_issue = True
                    self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                                         f"__init__ method, "
                                                         f"each_col --> {each_col}, "
                                                         f"each_col_dt_1 --> "
                                                         f"{each_col_dt_1}, "
                                                         f"each_col_dt_2 --> "
                                                         f"{each_col_dt_2}, "
                                                         f"allow_datatype_mapping --> "
                                                         f"{self.allow_datatype_mapping}")
                    if self.allow_datatype_mapping == "True":
                        if each_col_dt_1 in datatype_mapping_keys:
                            mapping_types = datatype_mapping[each_col_dt_1]
                            self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                                                 f"__init__ method, "
                                                                 f"each_col --> {each_col}, "
                                                                 f"mapping_types --> "
                                                                 f"{mapping_types}")
                            if each_col_dt_2 in mapping_types:
                                self.columns_in_both.append(each_col)
                    if column_schema_issue:
                        this_column_schema_issue = {
                            "TABLE_1_DATA_TYPE": f"{each_col_dt_1}",
                            "TABLE_2_DATA_TYPE": f"{each_col_dt_2}"
                        }
                        self.column_schema_issues[each_col] = this_column_schema_issue
                else:
                    self.columns_in_both.append(each_col)
            elif available_in_table_1 and not available_in_table_2:
                self.table_1_alone_columns.append(each_col)
                self.table_1_alone_columns_details.append(f"{each_col} --> {each_col_dt_1}")
            elif available_in_table_2 and not available_in_table_1:
                self.table_2_alone_columns.append(each_col)
                self.table_2_alone_columns_details.append(f"{each_col} --> {each_col_dt_2}")
            self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                                 f"__init__ method, "
                                                 f"each_col --> {each_col}, "
                                                 f"column_schema_issue --> "
                                                 f"{column_schema_issue}, "
                                                 f"available_in_table_1 --> "
                                                 f"{available_in_table_1}, "
                                                 f"available_in_table_2 --> "
                                                 f"{available_in_table_2}")

        self.sorted_list = sorted(list(chain(self.table_1_alone_columns,
                                             self.table_2_alone_columns,
                                             self.columns_in_both)))

        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"__init__ method, "
                                             f"type(table_1_schema) --> {type(self.table_1_schema)}, "
                                             f"table_1_schema --> {self.table_1_schema}, "
                                             f"base_table_schema --> {self.base_table_schema}, "
                                             f"table_2_schema --> {self.table_2_schema}, "
                                             f"compare_table_schema --> {self.compare_table_schema}, "
                                             f"columns_in_both --> {self.columns_in_both}, "
                                             f"column_schema_issues --> {self.column_schema_issues}, "
                                             f"table_1_records_count --> {self.table_1_records_count}, "
                                             f"table_2_records_count --> {self.table_2_records_count}")

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
        self.sample_record_count = int(self.validation_key_name_keys.get(
            "SAMPLE_RECORD_COUNT", global_constants.default_sample_record_counts))
        self.date_time_columns_list = self.validation_key_name_keys.get("DATE_TIME_COLUMNS", [])
        self.table_1_keys = self.validation_key_name_keys.get(
            "TABLE_1_DETAILS", {})
        self.table_2_keys = self.validation_key_name_keys.get(
            "TABLE_2_DETAILS", {})

        self.join_columns = self.convert_to_tuple()
        self._join_column_names = [name[0] for name in self.join_columns]

        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"__init__ method, "
                                             f"join_columns_list --> "
                                             f"{self.join_columns_list}, "
                                             f"join_column_names --> "
                                             f"{self._join_column_names}, "
                                             f"comparison_criteria --> "
                                             f"{self.comparison_criteria}, "
                                             f"comparison_type --> "
                                             f"{self.comparison_type}, "
                                             f"sample_record_count --> "
                                             f"{self.sample_record_count}, "
                                             f"date_time_columns_list --> "
                                             f"{self.date_time_columns_list}, "
                                             f"table_1_keys --> "
                                             f"{self.table_1_keys}, "
                                             f"table_2_keys --> "
                                             f"{self.table_2_keys}")

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

        self.pk_column_values_expr = ""
        self.join_pk_column_values_expr = ""
        self.column_schema_issues_keys = list(self.column_schema_issues.keys())
        now_run_summary_only = ""
        now_issues_in_join_column = ""
        for each_join_column in self.join_columns_list:
            self.pk_column_values_expr = self.common_operations.append_string(
                self.pk_column_values_expr,
                f"COALESCE(CAST(`{each_join_column}` AS STRING), '')",
                delimiter=global_constants.prinmary_key_concatenator
            )
            self.join_pk_column_values_expr = self.common_operations.append_string(
                self.join_pk_column_values_expr,
                f"COALESCE(CAST(table_name.`{each_join_column}` AS STRING), '')",
                delimiter=global_constants.prinmary_key_concatenator
            )
            if each_join_column in self.column_schema_issues_keys:
                now_run_summary_only = "True"
                now_issues_in_join_column = "SCHEMA ISSUES IN JOIN COLUMN"
            elif each_join_column not in self.columns_in_both:
                now_run_summary_only = "True"
                now_issues_in_join_column = "JOIN COLUMN NOT AVAILABLE IN BOTH TABLES"

        if now_issues_in_join_column:
            self.issues_in_join_column = now_issues_in_join_column
        else:
            self.issues_in_join_column = ""

        if now_run_summary_only:
            self.run_summary_only = now_run_summary_only
        else:
            self.run_summary_only = run_summary_only

        self.final_join_pk_column_values_expr = f"CONCAT({self.join_pk_column_values_expr})"
        self.final_select_pk_column_values_expr = f"CONCAT({self.pk_column_values_expr})"
        self.final_pk_column_values_expr = f"CONCAT({self.pk_column_values_expr}) " \
                                           f"AS primary_key_column_value"
        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"__init__ method, "
                                             f"final_pk_column_values_expr --> "
                                             f"{self.final_pk_column_values_expr} ")

        self.final_pk_column_names_value = ",".join(self.join_columns_list)

        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"__init__ method, "
                                             f"table_1_records_count --> {self.table_1_records_count}, "
                                             f"table_2_records_count --> {self.table_2_records_count}, "
                                             f"table_1_distinct_pk_count --> {self.table_1_distinct_pk_count}, "
                                             f"table_2_distinct_pk_count --> {self.table_2_distinct_pk_count}")

        self.spark = self.job_params.spark_session

        if self.table_1_records_count > 0 \
                and self.table_2_records_count > 0:
            if self.run_summary_only == "False":
                self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                                     f"__init__ method, "
                                                     f"columns_in_both --> {self.columns_in_both}")

                self.other_columns = []
                self.columns_compared = []
                for each_column in self.columns_in_both:
                    if each_column not in self.join_columns_list:
                        self.columns_compared.append(each_column)
                        if each_column in self.date_time_columns_list:
                            self.other_columns.append(
                                f"COALESCE(CAST(`{each_column}` AS STRING), '') AS `{each_column}`")
                        else:
                            self.other_columns.append(f"`{each_column}`")

                self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                                     f"__init__ method, "
                                                     f"other_columns --> {self.other_columns}, "
                                                     f"columns_compared --> {self.columns_compared}")
                self.run_sample_data_compare()
            else:
                self.get_sample_data_summary(summary_only=True)
        else:
            self.get_sample_data_summary(summary_only=True)
            self.common_operations.log_and_print(
                "Inside SparkDataCompare, "
                "__init__ method, "
                f"table_full_name_1 --> {self.table_1_full_name}, "
                f"table_1_records_count --> {self.table_1_records_count}, "
                f"table_full_name_2 --> {self.table_2_full_name}, "
                f"table_2_records_count --> {self.table_2_records_count}, "
                f"{self.table_1_full_name} OR {self.table_2_full_name}, "
                f"HAS ZERO RECORDS HENCE CANNOT DO DATA COMPARE",
                logger_type="error")

    def convert_to_tuple(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "convert_to_tuple method **********")
        ret_join_columns = []
        for val in self.join_columns_list:
            if isinstance(val, str):
                ret_join_columns.append((val, val))
            else:
                ret_join_columns.append(val)
        self.common_operations.log_and_print(
            "Inside SparkDataCompare, "
            "Inside convert_to_tuple, "
            f"ret_join_columns --> {ret_join_columns}")
        return ret_join_columns

    def create_select_statement(self, column_name):
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "create_select_statement method **********")
        if self.known_differences:
            match_type_comparison = ""
            for k in MatchType:
                match_type_comparison += " WHEN (A.{name}={match_value}) THEN '{match_name}'".format(
                    name=column_name, match_value=str(k.value), match_name=k.name
                )
            ret_str = "A.{name}_base, A.{name}_compare, " \
                      "(CASE WHEN (A.{name}={match_failure}) " \
                      "THEN False ELSE True END) AS {name}_match, " \
                      "(CASE {match_type_comparison} ELSE 'UNDEFINED' " \
                      "END) AS {name}_match_type ".format(name=column_name,
                                                          match_failure=MatchType.MISMATCH.value,
                                                          match_type_comparison=match_type_comparison
                                                          )
        else:
            ret_str = "A.{name}_base, A.{name}_compare, " \
                      "CASE WHEN (A.{name}={match_failure}) " \
                      "THEN False ELSE True END AS {name}_match ".format(name=column_name,
                                                                         match_failure=MatchType.MISMATCH.value
                                                                         )
        self.common_operations.log_and_print(
            "Inside SparkDataCompare, "
            "Inside create_select_statement, "
            f"ret_str --> {ret_str}")
        return ret_str

    def create_case_statement(self,
                              passed_column_name,
                              data_type_of_passed_column_in_base_df,
                              data_type_of_passed_column_in_compare_df):
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "create_case_statement method **********")
        equal_comparisons = ["(A.{name} IS NULL AND B.{name} IS NULL)"]
        known_diff_comparisons = ["(FALSE)"]

        if _is_comparable(data_type_of_passed_column_in_base_df, data_type_of_passed_column_in_compare_df):
            if (data_type_of_passed_column_in_base_df in NUMERIC_SPARK_TYPES) and (
                    data_type_of_passed_column_in_compare_df in NUMERIC_SPARK_TYPES
            ):  # numeric tolerance comparison
                equal_comparisons.append(
                    "((A.{name}=B.{name}) OR ((abs(A.{name}-B.{name}))<=("
                    + str(self.abs_tol)
                    + "+("
                    + str(self.rel_tol)
                    + "*abs(A.{name})))))"
                )
            else:  # non-numeric comparison
                equal_comparisons.append("((A.{name}=B.{name}))")

        if self.known_differences:
            new_input = "B.{name}"
            for kd in self.known_differences:
                if data_type_of_passed_column_in_compare_df in kd["types"]:
                    if "flags" in kd and "nullcheck" in kd["flags"]:
                        known_diff_comparisons.append(
                            "(("
                            + kd["transformation"].format(new_input, input=new_input)
                            + ") is null AND A.{name} is null)"
                        )
                    else:
                        known_diff_comparisons.append(
                            "(("
                            + kd["transformation"].format(new_input, input=new_input)
                            + ") = A.{name})"
                        )

        case_string = (
                "( CASE WHEN ("
                + " OR ".join(equal_comparisons)
                + ") THEN {match_success} WHEN ("
                + " OR ".join(known_diff_comparisons)
                + ") THEN {match_known_difference} ELSE {match_failure} END) "
                + "AS {name}, A.{name} AS {name}_base, B.{name} AS {name}_compare"
        ).format(
            name=passed_column_name,
            match_success=MatchType.MATCH.value,
            match_known_difference=MatchType.KNOWN_DIFFERENCE.value,
            match_failure=MatchType.MISMATCH.value,
        )
        self.common_operations.log_and_print(
            "Inside SparkDataCompare, "
            "Inside _create_case_statement, "
            f"case_string --> {case_string}")
        return case_string

    def generate_select_statement(self, passed_match_data=True):
        ret_select_statement = ""
        for column_name in self.sorted_list:
            if column_name in self.columns_compared:
                if passed_match_data:
                    ret_select_statement = ret_select_statement + ",".join(
                        [self.create_case_statement(column_name,
                                                    self.base_table_schema[column_name],
                                                    self.compare_table_schema[column_name]
                                                    )]
                    )
                else:
                    ret_select_statement = ret_select_statement + ",".join(
                        [self.create_select_statement(column_name=column_name)]
                    )
            elif column_name in self.table_1_alone_columns:
                ret_select_statement = ret_select_statement + ",".join(["A." + column_name])

            elif column_name in self.table_2_alone_columns:
                if passed_match_data:
                    ret_select_statement = ret_select_statement + ",".join(["B." + column_name])
                else:
                    ret_select_statement = ret_select_statement + ",".join(["A." + column_name])
            elif column_name in self._join_column_names:
                ret_select_statement = ret_select_statement + ",".join(["A." + column_name])

            if column_name != self.sorted_list[-1]:
                ret_select_statement = ret_select_statement + " , "
        self.common_operations.log_and_print(
            "Inside SparkDataCompare, "
            "Inside generate_select_statement, "
            f"ret_select_statement --> {ret_select_statement}")
        return ret_select_statement

    def convert_row_missing_dataframe_to_desired_patter(self, passed_dataframe,
                                                        available_in, missing_in,
                                                        passed_name_of_dataframe):
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "convert_row_missing_dataframe_to_desired_patter "
                                             "method **********")
        self.common_operations.display_data_frame(passed_dataframe, passed_name_of_dataframe)

        unq_rows_fin = passed_dataframe.selectExpr(
            *self.other_columns,
            f"'{global_constants.run_date_time}' AS run_date_time",
            f"'{self.job_params.validation_key_name}' AS key_name",
            f"'{available_in}' AS available_in",
            f"'{missing_in}' AS missing_in",
            f"{self.final_pk_column_values_expr}",
            f"'{self.final_pk_column_names_value}' AS primary_key_column_names"
        ).distinct()

        self.common_operations.display_data_frame(unq_rows_fin,
                                                  f"{passed_name_of_dataframe}_unq_rows_fin")

        results_json = unq_rows_fin.toJSON().map(lambda j: json.loads(j)).collect()
        for each_value in results_json:
            self.common_operations.log_and_print(f"Inside SparkDataCompare, "
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
            self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                                 f"convert_row_missing_dataframe_to_desired_patter "
                                                 f"method, row_differences_dict --> {row_differences_dict}")
            self.row_differences_dict_list.append(row_differences_dict)

        self.common_operations.log_and_print(
            "Inside SparkDataCompare"
            "convert_row_missing_dataframe_to_desired_patter "
            "method, "
            f"len(row_differences_dict_list) --> {len(self.row_differences_dict_list)}, "
            f"row_differences_dict_list --> {self.row_differences_dict_list}")

    def get_sample_data_row_missing(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "get_sample_data_row_missing method **********")
        base_rows = self.table_1_sample_data.selectExpr(
            self.final_pk_column_values_expr).subtract(
            self.table_2_sample_data.selectExpr(self.final_pk_column_values_expr)
        )
        self.common_operations.log_and_print("Inside SparkDataCompare, after base_rows")
        base_rows.createOrReplaceTempView("baseRows")
        self.table_1_sample_data.createOrReplaceTempView("baseTable")
        join_condition = (f"{str(self.final_join_pk_column_values_expr).replace('table_name.', 'A.')}="
                          f"B.primary_key_column_value")
        sql_query = "select A.* from baseTable as A, baseRows as B where {}".format(
            join_condition
        )
        self.common_operations.log_and_print("Inside SparkDataCompare, after base_rows, "
                                             f"sql_query --> {sql_query}")
        table_1_df_unq_rows = self.spark.sql(sql_query)
        self.table_1_unq_rows_count = table_1_df_unq_rows.count()
        self.table_1_unq_rows_columns = table_1_df_unq_rows.columns
        self.common_operations.log_and_print("Inside SparkDataCompare, "
                                             "get_sample_data_row_missing method, "
                                             "table_1_unq_rows_count --> "
                                             f"{self.table_1_unq_rows_count}, "
                                             f"table_1_unq_rows_columns --> "
                                             f"{self.table_1_unq_rows_columns}")
        if self.table_1_unq_rows_count > 0:
            self.convert_row_missing_dataframe_to_desired_patter(
                table_1_df_unq_rows.limit(self.sample_record_count),
                f"{self.table_1_full_name}",
                f"{self.table_2_full_name}",
                "table_1_df_unq_rows"
            )
        compare_rows = self.table_2_sample_data.selectExpr(
            self.final_pk_column_values_expr).subtract(
            self.table_1_sample_data.selectExpr(self.final_pk_column_values_expr)
        )
        self.common_operations.log_and_print("Inside SparkDataCompare, after compare_rows")
        compare_rows.createOrReplaceTempView("compareRows")
        self.table_2_sample_data.createOrReplaceTempView("compareTable")
        sql_query = "select A.* from compareTable as A, compareRows as B where {}".format(
            join_condition
        )
        self.common_operations.log_and_print("Inside SparkDataCompare, after compare_rows, "
                                             f"sql_query --> {sql_query}")
        table_2_df_unq_rows = self.spark.sql(sql_query)

        self.table_2_unq_rows_count = table_2_df_unq_rows.count()
        self.table_2_unq_rows_columns = table_2_df_unq_rows.columns
        self.common_operations.log_and_print("Inside SparkDataCompare, "
                                             "get_sample_data_row_missing method, "
                                             "table_2_unq_rows_count --> "
                                             f"{self.table_2_unq_rows_count}, "
                                             f"table_2_unq_rows_columns --> "
                                             f"{self.table_2_unq_rows_columns}")
        if self.table_2_unq_rows_count > 0:
            self.convert_row_missing_dataframe_to_desired_patter(
                table_2_df_unq_rows.limit(self.sample_record_count),
                f"{self.table_2_full_name}",
                f"{self.table_1_full_name}",
                "table_2_df_unq_rows"
            )

        self.common_operations.log_and_print("Inside SparkDataCompare, "
                                             "get_sample_data_row_missing method, "
                                             "len(row_differences_dict_list) --> "
                                             f"{len(self.row_differences_dict_list)}")
        if len(self.row_differences_dict_list) > 0:
            df_row_differences = self.spark.createDataFrame(self.row_differences_dict_list)
            self.common_operations.display_data_frame(df_row_differences, "df_row_differences")
            sample_data_row_missing_table_name = self.now_validation_type_keys.get(
                global_constants.sample_data_row_missing_results_key_name)
            self.job_params.results_endpoint.save_results(
                df_row_differences, sample_data_row_missing_table_name)

    def get_sample_data_column_mismatch(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "get_sample_data_column_mismatch method **********")

        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"get_sample_data_column_mismatch method, "
                                             f"table_1_alone_columns --> {self.table_1_alone_columns}, "
                                             f"table_2_alone_columns --> {self.table_2_alone_columns}, "
                                             f"columns_compared --> {self.columns_compared}, "
                                             f"sorted_list --> {self.sorted_list}, "
                                             f"columns_compared --> {self.columns_compared}, "
                                             f"table_1_schema --> {self.table_1_schema}, "
                                             f"base_table_schema --> {self.base_table_schema}, "
                                             f"table_2_schema --> {self.table_2_schema}, "
                                             f"compare_table_schema --> {self.compare_table_schema}")

        join_condition = (f"{str(self.final_join_pk_column_values_expr).replace('table_name.', 'A.')}="
                          f"{str(self.final_join_pk_column_values_expr).replace('table_name.', 'B.')}")
        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"get_sample_data_column_mismatch method, "
                                             f"join_condition --> {join_condition}")

        select_statement = self.generate_select_statement()

        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"get_sample_data_column_mismatch method, "
                                             f"select_statement --> {select_statement}")

        join_query = r"""
               SELECT {}
               FROM base_table A
               JOIN compare_table B
               ON {}""".format(
            select_statement, join_condition
        )

        joined_dataframe = self.spark.sql(join_query)

        joined_dataframe.cache()
        common_row_count = joined_dataframe.count()
        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"get_sample_data_column_mismatch method, "
                                             f"joined_dataframe count --> {common_row_count}, "
                                             f"joined_dataframe schema --> {joined_dataframe.schema}")
        self.common_operations.display_data_frame(joined_dataframe,
                                                  f"joined_dataframe")
        joined_dataframe.createOrReplaceTempView("full_matched_table")

        select_statement_after = self.generate_select_statement(passed_match_data=False)

        select_query = """SELECT {} FROM full_matched_table A""".format(select_statement_after)

        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"get_sample_data_column_mismatch method, "
                                             f"select_statement_after --> {select_statement_after}, "
                                             f"select_query --> {select_query}")

        all_matched_rows = self.spark.sql(select_query).orderBy(self._join_column_names)
        all_matched_rows.cache()
        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"get_sample_data_column_mismatch method, "
                                             f"all_matched_rows count --> {all_matched_rows.count()}, "
                                             f"all_matched_rows schema --> {all_matched_rows.schema}")
        self.common_operations.display_data_frame(all_matched_rows,
                                                  f"all_matched_rows")
        all_matched_rows.createOrReplaceTempView("matched_table")

        where_cond = " OR ".join(["A." + name + "_match= False" for name in self.columns_compared])
        mismatch_query = """SELECT * FROM matched_table A WHERE {}""".format(where_cond)

        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"get_sample_data_column_mismatch method, "
                                             f"mismatch_query --> {mismatch_query}")

        df_mismatch_all = self.spark.sql(mismatch_query).orderBy(self._join_column_names)
        df_mismatch_all.cache()
        self.common_operations.log_and_print(f"Inside SparkDataCompare, "
                                             f"get_sample_data_column_mismatch method, "
                                             f"df_mismatch_all count --> {df_mismatch_all.count()}, "
                                             f"df_mismatch_all schema --> {df_mismatch_all.schema}")
        self.mismatch_rows_count = df_mismatch_all.count()
        self.all_mismatch_columns = df_mismatch_all.columns

        if len(df_mismatch_all.head(1)) != 0:
            self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                 "get_sample_data_column_mismatch method, "
                                                 "Rows that are not matched in both data, "
                                                 f"mismatch_rows_count --> {self.mismatch_rows_count}, "
                                                 f"all_mismatch_columns --> {self.all_mismatch_columns}")
            self.common_operations.display_data_frame(df_mismatch_all, "df_mismatch_all")
            for column_name in self.columns_compared:
                self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                     "get_sample_data_column_mismatch method, "
                                                     f"COMPARING column_name --> {column_name}")
                if f"{column_name}_match" in self.all_mismatch_columns:
                    self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                         "get_sample_data_column_mismatch method, "
                                                         f"COMPARING column_name --> "
                                                         f"{column_name}, "
                                                         f"{column_name}_match FOUND IN "
                                                         f"{self.all_mismatch_columns}")
                    col_df = df_mismatch_all.select(
                        *self.join_columns_list,
                        f"A.{column_name}_base",
                        f"A.{column_name}_compare"
                    ).where(f"{column_name}_match == 'false'").distinct()
                    now_col_diff_count = col_df.count()
                    self.column_wise_mismatch_counts[column_name] = now_col_diff_count
                    self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                         "get_sample_data_column_mismatch method, "
                                                         f"COMPARING column_name --> "
                                                         f"{column_name}, "
                                                         f"{column_name}_match FOUND IN "
                                                         f"{self.all_mismatch_columns}, "
                                                         f"now_col_diff_count --> {now_col_diff_count}")
                    if len(col_df.head(1)) != 0:
                        self.mis_matched_column.append(column_name)
                        self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                             "get_sample_data_column_mismatch method, "
                                                             "\n ******* "
                                                             f"MISMATCH IN COLUMN --> "
                                                             f"'{column_name}' *********")
                        self.common_operations.display_data_frame(col_df, f"col_df_{column_name}")
                        results_col_df = col_df.limit(self.sample_record_count). \
                            toJSON().map(lambda j: json.loads(j)).collect()
                        for each_col_value in results_col_df:
                            column_value_from_table_1 = each_col_value.pop(f"{column_name}_base", "")
                            column_value_from_table_2 = each_col_value.pop(f"{column_name}_compare", "")
                            column_data_type_from_table_1 = self.table_1_schema[column_name]
                            column_data_type_from_table_2 = self.table_2_schema[column_name]
                            self.common_operations.log_and_print("Inside SparkDataCompare, "
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
                                "key_name": f"{self.job_params.validation_key_name}",
                                "column_name": f"{column_name}",
                                "primary_key_column_names": f"{self.final_pk_column_names_value}",
                                "primary_key_column_value": f"{each_col_value}",
                                "table_name_1": f"{self.table_1_full_name}",
                                "table_name_2": f"{self.table_2_full_name}",
                                "column_data_type_from_table_1": f"{column_data_type_from_table_1}",
                                "column_data_type_from_table_2": f"{column_data_type_from_table_2}",
                                "column_value_from_table_1": f"{column_value_from_table_1}",
                                "column_value_from_table_2": f"{column_value_from_table_2}"
                            }
                            self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                                 "get_sample_data_column_mismatch method, "
                                                                 "col_diff_dict --> {col_diff_dict}")
                            self.col_diff_dict_list.append(col_diff_dict)
                        self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                             "get_sample_data_column_mismatch method, "
                                                             "len(col_diff_dict_list) --> "
                                                             f"{len(self.col_diff_dict_list)}, "
                                                             f"col_diff_dict_list --> "
                                                             f"{self.col_diff_dict_list}")
                    else:
                        self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                             "get_sample_data_column_mismatch method, "
                                                             f"\n ******* "
                                                             f"COLUMN '{column_name}' MATCH, "
                                                             f"NO ISSUES *********")
                        self.matched_column.append(column_name)
                else:
                    self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                         "get_sample_data_column_mismatch method, "
                                                         "COMPARING column_name --> {column_name}, "
                                                         "{column_name}_match, "
                                                         "NOT FOUND IN {self.all_mismatch_columns}")
        else:
            self.common_operations.log_and_print("Inside SparkDataCompare, "
                                                 "get_sample_data_column_mismatch method, "
                                                 "\n ******* "
                                                 "COLUMNS ARE COMPLETELY MATCHING, "
                                                 "NO ISSUES *********")

        self.common_operations.log_and_print("Inside SparkDataCompare, "
                                             "get_sample_data_column_mismatch method, "
                                             "len(col_diff_dict_list) --> "
                                             f"{len(self.col_diff_dict_list)}")
        if len(self.col_diff_dict_list) > 0:
            df_coll_differences = self.spark.createDataFrame(self.col_diff_dict_list)
            self.common_operations.display_data_frame(df_coll_differences, "df_coll_differences")
            sample_data_column_mismatch_table_name = self.now_validation_type_keys.get(
                global_constants.sample_data_column_mismatch_results_key_name)
            self.job_params.results_endpoint.save_results(
                df_coll_differences, sample_data_column_mismatch_table_name)

    def form_summary_dict(self, passed_attribute_name, passed_attribute_value):
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "form_summary_dict method **********")
        self.common_operations.log_and_print("Inside SparkDataCompare, "
                                             "form_summary_dict method, "
                                             "passed_attribute_name --> "
                                             f"{passed_attribute_name}, "
                                             f"passed_attribute_value --> "
                                             f"{passed_attribute_value}")

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
        self.summary_dict_list.append(now_summary_dict)

    def get_sample_data_summary(self, summary_only=False):
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "get_sample_data_summary method **********")

        self.form_summary_dict("run_time_params", self.job_params.args)

        self.form_summary_dict("records_count_in_table_1", self.table_1_records_count)
        self.form_summary_dict("records_count_in_table_2", self.table_2_records_count)

        self.form_summary_dict("distinct_primary_key_count_in_table_1",
                               self.table_1_distinct_pk_count)
        self.form_summary_dict("distinct_primary_key_count_in_table_2",
                               self.table_2_distinct_pk_count)
        self.form_summary_dict("columns_with_data_type_issues",
                               f"{self.column_schema_issues}")
        self.form_summary_dict("count_of_columns_with_data_type_issues",
                               f"{len(self.column_schema_issues_keys)}")
        self.form_summary_dict("columns_in_table_1_alone",
                               f"{self.table_1_alone_columns_details}")
        self.form_summary_dict("columns_in_table_2_alone",
                               f"{self.table_2_alone_columns_details}")
        self.form_summary_dict("issues_in_join_column",
                               f"{self.issues_in_join_column}")
        self.form_summary_dict("table_1_query",
                               f"{self.table_1_query}")
        self.form_summary_dict("table_2_query",
                               f"{self.table_2_query}")
        if self.limit_sample_data == "True":
            table_1_limit = self.job_params.get_params("table_1_limit")
            table_2_limit = self.job_params.get_params("table_2_limit")
            self.form_summary_dict("table_1_limit",
                                   table_1_limit)
            self.form_summary_dict("table_2_limit",
                                   table_2_limit)

        if not summary_only:
            self.form_summary_dict("count_of_rows_missing_in_table_1", self.table_2_unq_rows_count)
            self.form_summary_dict("count_of_rows_missing_in_table_2", self.table_1_unq_rows_count)

            self.form_summary_dict("count_of_rows_with_mismatch", self.mismatch_rows_count)

            self.form_summary_dict("number_of_columns_in_table_1", len(self.table_1_schema))
            self.form_summary_dict("number_of_columns_in_table_2", len(self.table_2_schema))
            self.form_summary_dict("number_of_common_columns", len(self.columns_compared))

            self.form_summary_dict("number_of_columns_in_table_1_not_in_table_2",
                                   len(self.table_1_alone_columns))
            self.form_summary_dict("number_of_columns_in_table_2_not_in_table_1",
                                   len(self.table_2_alone_columns))

            self.form_summary_dict("number_of_columns_which_matched",
                                   len(self.matched_column))
            self.form_summary_dict("number_of_columns_which_have_mismatch",
                                   len(self.mis_matched_column))

            self.form_summary_dict("columns_which_matched",
                                   ', '.join(self.matched_column))
            self.form_summary_dict("columns_which_have_mismatch",
                                   ', '.join(self.mis_matched_column))

            self.form_summary_dict("columns_wise_count_of_records_with_mismatch",
                                   f"{self.column_wise_mismatch_counts}")

        df_summary = self.spark.createDataFrame(self.summary_dict_list)
        self.common_operations.display_data_frame(df_summary, "df_summary")
        sample_data_summary_table_name = self.now_validation_type_keys.get(
            global_constants.sample_data_summary_results_key_name)
        self.job_params.results_endpoint.save_results(
            df_summary, sample_data_summary_table_name)

    def run_sample_data_compare(self):
        self.common_operations.log_and_print("********** Inside SparkDataCompare "
                                             "run_sample_data_compare method **********")
        self.get_sample_data_row_missing()
        self.get_sample_data_column_mismatch()
        self.get_sample_data_summary()
