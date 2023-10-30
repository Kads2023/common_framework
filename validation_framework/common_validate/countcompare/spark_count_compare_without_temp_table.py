from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations
from validation_framework.common_validate.countcompare.spark_count_compare import SparkCountCompare

from validation_framework.common_validate.utils import global_constants


class SparkCountCompareWithoutTempTable(SparkCountCompare):

    def __init__(self,
                 passed_job_params: JobParam,
                 passed_common_operations: CommonOperations
                 ):
        print("********** Inside SparkCountCompareWithoutTempTable's __init__ method ********** ")
        super().__init__(passed_job_params, passed_common_operations)
        self.table_1_endpoint = self.job_params.table_1_endpoint
        self.table_2_endpoint = self.job_params.table_2_endpoint
        if self.where_clause_to_be_used_for_counts == "True":
            self.final_table_1_query = f"({self.table_1_query})"
            self.final_table_2_query = f"({self.table_2_query})"
        else:
            self.final_table_1_query = self.table_1_full_name
            self.final_table_2_query = self.table_2_full_name
        self.run_counts_compare()

    def check_total_counts(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_total_counts method **********")
        check_count_query = f"SELECT " \
                            f"count(*) as count_of_records " \
                            f"FROM " \
                            f"table_name "
        self.queries_executed["count_of_records"] = check_count_query
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_total_counts method, "
                                             f"check_count_query --> "
                                             f"{check_count_query}")
        self.table_1_count_of_records = int(self.table_1_endpoint.execute_query_and_get_counts(
            check_count_query.replace(
                "table_name",
                self.final_table_1_query
            ),
            "check_total_counts",
            "count_of_records"
        ))
        self.table_2_count_of_records = int(self.table_2_endpoint.execute_query_and_get_counts(
            check_count_query.replace(
                "table_name",
                self.final_table_2_query
            ),
            "check_total_counts",
            "count_of_records"
        ))
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_total_counts method, "
                                             f"table_1_count_of_records --> "
                                             f"{self.table_1_count_of_records}, "
                                             f"table_2_count_of_records --> "
                                             f"{self.table_2_count_of_records}")
        self.generic_counts_calculated["count_of_records"] = {
            f"{self.table_1_full_name}": self.table_1_count_of_records,
            f"{self.table_2_full_name}": self.table_2_count_of_records
        }

    def check_null_counts(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_null_counts method **********")
        check_null_query = f"SELECT " \
                           f"count(*) as count_of_null_records " \
                           f"FROM " \
                           f"table_name " \
                           f"WHERE " \
                           f"`each_check_null_column` IS NULL"
        self.queries_executed["count_of_null"] = check_null_query
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_null_counts method, "
                                             f"check_null_query --> "
                                             f"{check_null_query}")
        if len(self.check_null_columns_list) != 0:
            for each_check_null_column in self.check_null_columns_list:
                table_1_count_of_null = int(self.table_1_endpoint.execute_query_and_get_counts(
                    check_null_query.replace(
                        "table_name",
                        self.final_table_1_query
                    ).replace(
                        "each_check_null_column",
                        each_check_null_column
                    ),
                    "check_null_counts",
                    "count_of_null_records"
                ))
                table_2_count_of_null = int(self.table_2_endpoint.execute_query_and_get_counts(
                    check_null_query.replace(
                        "table_name",
                        self.final_table_2_query
                    ).replace(
                        "each_check_null_column",
                        each_check_null_column
                    ),
                    "check_null_counts",
                    "count_of_null_records"
                ))
                self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                                     f"check_null_counts method, "
                                                     f"table_1_count_of_null --> "
                                                     f"{table_1_count_of_null}, "
                                                     f"table_2_count_of_null --> "
                                                     f"{table_2_count_of_null}")
                self.null_counts_dict[each_check_null_column] = {
                    f"{self.table_1_full_name}": table_1_count_of_null,
                    f"{self.table_2_full_name}": table_2_count_of_null
                }
        else:
            self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                                 f"check_null_counts method, "
                                                 f"len(check_null_columns_list) --> "
                                                 f"{len(self.check_null_columns_list)} IS ZERO")
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_null_counts method, "
                                             f"null_counts_dict --> "
                                             f"{self.null_counts_dict}")
        self.generic_counts_calculated["count_of_null"] = self.null_counts_dict

    def check_pk_counts(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_pk_counts method **********")
        check_pk_counts_query = "SELECT " \
                                f"count(CONCAT({self.pk_column_values_expr})) as pk_counts " \
                                f"FROM " \
                                "table_name "
        self.queries_executed["pk_counts"] = check_pk_counts_query
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_pk_counts method, "
                                             f"check_pk_counts_query --> "
                                             f"{check_pk_counts_query}")
        self.table_1_pk_counts = int(self.table_1_endpoint.execute_query_and_get_counts(
            check_pk_counts_query.replace(
                "table_name",
                self.final_table_1_query
            ),
            "check_pk_counts",
            "pk_counts"
        ))
        self.table_2_pk_counts = int(self.table_2_endpoint.execute_query_and_get_counts(
            check_pk_counts_query.replace(
                "table_name",
                self.final_table_2_query
            ),
            "check_pk_counts",
            "pk_counts"
        ))
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_pk_counts method, "
                                             f"table_1_pk_counts --> "
                                             f"{self.table_1_pk_counts}, "
                                             f"table_2_pk_counts --> "
                                             f"{self.table_2_pk_counts}")
        self.generic_counts_calculated["pk_counts"] = {
            f"{self.table_1_full_name}": self.table_1_pk_counts,
            f"{self.table_2_full_name}": self.table_2_pk_counts
        }

    def check_distinct_pk_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_distinct_pk_count method **********")
        check_duplicates_pk_query = "SELECT " \
                                    f"count(distinct CONCAT({self.pk_column_values_expr})) " \
                                    f"as distinct_pk_counts " \
                                    f"FROM " \
                                    f"table_name "
        self.queries_executed["distinct_pk_counts"] = check_duplicates_pk_query
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_pk_duplicates_count method, "
                                             f"check_duplicates_pk_query --> "
                                             f"{check_duplicates_pk_query}")
        self.table_1_distinct_pk_count = int(self.table_1_endpoint.execute_query_and_get_counts(
            check_duplicates_pk_query.replace(
                "table_name",
                self.final_table_1_query
            ),
            "check_distinct_pk_count",
            "distinct_pk_counts"
        ))
        self.table_2_distinct_pk_count = int(self.table_2_endpoint.execute_query_and_get_counts(
            check_duplicates_pk_query.replace(
                "table_name",
                self.final_table_2_query
            ),
            "check_distinct_pk_count",
            "distinct_pk_counts"
        ))
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_distinct_pk_count method, "
                                             f"table_1_distinct_pk_count --> "
                                             f"{self.table_1_distinct_pk_count}, "
                                             f"table_2_distinct_pk_count --> "
                                             f"{self.table_2_distinct_pk_count}")
        self.generic_counts_calculated["distinct_pk_counts"] = {
            f"{self.table_1_full_name}": self.table_1_distinct_pk_count,
            f"{self.table_2_full_name}": self.table_2_distinct_pk_count
        }

    def check_pk_null_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_pk_null_count method **********")
        check_pk_null_query = "SELECT " \
                              "count(*) as count_of_pk_null_records " \
                              "FROM " \
                              f"table_name " \
                              f"WHERE " \
                              f"{self.pk_null_values_check}"
        self.queries_executed["count_of_pk_null_records"] = check_pk_null_query
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_pk_null_count method, "
                                             f"check_pk_null_query --> "
                                             f"{check_pk_null_query}")
        self.table_1_pk_null_counts = int(self.table_1_endpoint.execute_query_and_get_counts(
            check_pk_null_query.replace(
                "table_name",
                self.final_table_1_query
            ),
            "check_pk_null_count",
            "count_of_pk_null_records"
        ))
        self.table_2_pk_null_counts = int(self.table_2_endpoint.execute_query_and_get_counts(
            check_pk_null_query.replace(
                "table_name",
                self.final_table_2_query
            ),
            "check_pk_null_count",
            "count_of_pk_null_records"
        ))
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_pk_null_count method, "
                                             f"table_1_pk_null_counts --> "
                                             f"{self.table_1_pk_null_counts}, "
                                             f"table_2_pk_null_counts --> "
                                             f"{self.table_2_pk_null_counts}")
        self.generic_counts_calculated["count_of_pk_null"] = {
            f"{self.table_1_full_name}": self.table_1_pk_null_counts,
            f"{self.table_2_full_name}": self.table_2_pk_null_counts
        }

    def check_date_time_column_null_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_date_time_column_null_count method **********")
        check_date_time_column_null_query = "SELECT " \
                                            "count(*) as count_date_time_column_null " \
                                            "FROM " \
                                            "table_name " \
                                            "WHERE " \
                                            "`each_date_time_column_name` IS NULL"
        self.queries_executed["count_date_time_column_null"] = check_date_time_column_null_query
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_date_time_column_null_count method, "
                                             f"check_date_time_column_null_query --> "
                                             f"{check_date_time_column_null_query}")
        if len(self.date_time_columns_list) != 0:
            for each_date_time_column_name in self.date_time_columns_list:
                table_1_count_of_null = int(self.table_1_endpoint.execute_query_and_get_counts(
                    check_date_time_column_null_query.replace(
                        "table_name",
                        self.final_table_1_query
                    ).replace(
                        "each_date_time_column_name",
                        each_date_time_column_name
                    ),
                    "check_date_time_column_null_count",
                    "count_date_time_column_null"
                ))
                table_2_count_of_null = int(self.table_2_endpoint.execute_query_and_get_counts(
                    check_date_time_column_null_query.replace(
                        "table_name",
                        self.final_table_2_query
                    ).replace(
                        "each_date_time_column_name",
                        each_date_time_column_name
                    ),
                    "check_date_time_column_null_count",
                    "count_date_time_column_null"
                ))
                self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                                     f"check_date_time_column_null_count "
                                                     f"method, "
                                                     f"table_1_count_of_null --> "
                                                     f"{table_1_count_of_null}, "
                                                     f"table_2_count_of_null --> "
                                                     f"{table_2_count_of_null}")
                self.date_time_column_null_counts_dict[each_date_time_column_name] = {
                    f"{self.table_1_full_name}": table_1_count_of_null,
                    f"{self.table_2_full_name}": table_2_count_of_null
                }
        else:
            self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                                 f"check_date_time_column_null_count method, "
                                                 f"len(date_time_columns_list) --> "
                                                 f"{len(self.date_time_columns_list)} IS ZERO")
        self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                             f"check_date_time_column_null_count "
                                             f"method, "
                                             f"date_time_column_null_counts_dict --> "
                                             f"{self.date_time_column_null_counts_dict}")
        self.generic_counts_calculated["count_date_time_column_null"] = self.date_time_column_null_counts_dict

    def generic_process_date_wise(self, passed_query, passed_column_name):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "generic_process_date_wise method **********")
        self.common_operations.log_and_print("Inside SparkCountCompareWithoutTempTable "
                                             "generic_process_date_wise method, "
                                             "passed_column_name --> {passed_column_name}, "
                                             "passed_query --> {passed_query}")
        table_1_counts_by_group = \
            self.table_1_endpoint.execute_query_and_get_data_frame(
                passed_query.replace(
                    "table_name",
                    self.final_table_1_query
                ),
                f"{passed_column_name}"
            ).selectExpr(
                *self.group_by_all_columns_outer_select_list,
                "distinct_count_of_records AS table_1_distinct_count_of_records",
                "count_of_records AS table_1_count_of_records",
                "(count_of_records - distinct_count_of_records) AS table_1_count_difference"
            )

        self.common_operations.display_data_frame(
            table_1_counts_by_group,
            f"table_1_counts_by_group_{passed_column_name}"
        )

        table_2_counts_by_group = \
            self.table_2_endpoint.execute_query_and_get_data_frame(
                passed_query.replace(
                    "table_name",
                    self.final_table_2_query
                ),
                f"{passed_column_name}"
            ).selectExpr(
                *self.group_by_all_columns_outer_select_list,
                "distinct_count_of_records AS table_2_distinct_count_of_records",
                "count_of_records AS table_2_count_of_records",
                "(count_of_records - distinct_count_of_records) AS table_2_count_difference"
            )

        self.common_operations.display_data_frame(
            table_2_counts_by_group,
            f"table_2_counts_by_group_{passed_column_name}"
        )

        joined_data = table_1_counts_by_group.join(
            table_2_counts_by_group,
            self.group_by_all_columns_outer_select_list
        ).selectExpr(
            f"'{global_constants.run_date_time}' AS run_date_time",
            f"'{self.job_params.validation_key_name}' AS key_name",
            f"'{self.table_1_full_name}' AS table_name_1",
            f"'{self.table_2_full_name}' AS table_name_2",
            f"'{passed_column_name}' AS distinct_count_of_column",
            f"'{self.group_by_column_names}' AS group_by_column_names",
            f"{self.group_by_column_values} AS group_by_column_values",
            "table_1_distinct_count_of_records",
            "table_1_count_of_records",
            "table_1_count_difference",
            "table_2_distinct_count_of_records",
            "table_2_count_of_records",
            "table_2_count_difference",
            "(table_1_distinct_count_of_records - table_2_distinct_count_of_records) "
            "AS count_of_distinct_count_difference",
            "(table_1_count_of_records - table_2_count_of_records) AS count_of_records_difference"
        )

        self.common_operations.display_data_frame(
            joined_data,
            f"joined_data_{passed_column_name}"
        )

        final_data = joined_data.selectExpr(
            "run_date_time",
            "key_name",
            "table_name_1",
            "table_name_2",
            "distinct_count_of_column",
            "group_by_column_names",
            "group_by_column_values",
            "CAST(table_1_distinct_count_of_records AS STRING) AS table_1_distinct_count_of_records",
            "CAST(table_1_count_of_records AS STRING) AS table_1_count_of_records",
            "CAST(table_1_count_difference AS STRING) AS table_1_count_difference",
            "CAST(table_2_distinct_count_of_records AS STRING) AS table_2_distinct_count_of_records",
            "CAST(table_2_count_of_records AS STRING) AS table_2_count_of_records",
            "CAST(table_2_count_difference AS STRING) AS table_2_count_difference",
            "CAST(count_of_distinct_count_difference AS STRING) AS distinct_count_of_records_difference",
            "CAST(count_of_records_difference AS STRING) AS count_of_records_difference",
            "CASE WHEN (count_of_distinct_count_difference == 0) "
            "THEN 'True' ELSE 'False' END AS distinct_count_of_records_matched",
            "CASE WHEN (count_of_records_difference == 0) "
            "THEN 'True' ELSE 'False' END AS count_of_records_matched"
        )

        self.common_operations.display_data_frame(
            final_data,
            f"final_data_{passed_column_name}"
        )
        final_data_count = final_data.count()
        final_data_schema = self.common_operations.list_of_tuples_to_dict(
            final_data.dtypes)
        self.common_operations.log_and_print("Inside SparkCountCompareWithoutTempTable "
                                             "generic_process_date_wise method, "
                                             "passed_column_name --> "
                                             f"{passed_column_name}, "
                                             f"final_data_count --> "
                                             f"{final_data_count}, "
                                             f"final_data_schema --> "
                                             f"{final_data_schema}")
        if final_data_count > 0:
            count_details_table_name = self.now_validation_type_keys.get(
                global_constants.count_details_results_key_name)
            self.job_params.results_endpoint.save_results(
                final_data, count_details_table_name)

    def check_group_by_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_group_by_count method **********")
        if self.group_by_all_columns_outer_select and \
                self.group_by_all_columns_inner_select:
            counts_by_group_query = f"SELECT " \
                                    f"count(distinct CONCAT({self.pk_column_values_expr})) " \
                                    f"as distinct_count_of_records, " \
                                    f"count(*) as count_of_records, " \
                                    f"{self.group_by_all_columns_outer_select} " \
                                    f"FROM " \
                                    f"(SELECT *, " \
                                    f"{self.group_by_all_columns_inner_select} " \
                                    f"FROM " \
                                    f"table_name) " \
                                    f"GROUP BY " \
                                    f"{self.group_by_all_columns_outer_select} " \
                                    f"ORDER BY " \
                                    f"{self.group_by_all_columns_outer_select} "
            self.queries_executed["counts_by_group_query"] = counts_by_group_query
            self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                                 f"check_group_by_count method, "
                                                 f"counts_by_group_query --> "
                                                 f"{counts_by_group_query}")
            self.generic_process_date_wise(counts_by_group_query, self.final_pk_column_names_value)
        else:
            self.common_operations.log_and_print("Inside SparkCountCompareWithoutTempTable "
                                                 "check_group_by_count method, "
                                                 "group_by_all_columns_outer_select --> "
                                                 f"{self.group_by_all_columns_outer_select} OR "
                                                 f"group_by_all_columns_inner_select --> "
                                                 f"{self.group_by_all_columns_inner_select}, "
                                                 f"IS EMPTY")

    def check_type_code_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_type_code_count method **********")
        if len(self.type_code_columns_list) != 0 and \
                self.group_by_all_columns_outer_select and \
                self.group_by_all_columns_inner_select:
            counts_by_each_type_code_column_query = f"SELECT " \
                                                    f"count(distinct `each_type_code_column`)  " \
                                                    f"as distinct_count_of_records, " \
                                                    f"count(*) as count_of_records, " \
                                                    f"{self.group_by_all_columns_outer_select} " \
                                                    f"FROM " \
                                                    f"(SELECT " \
                                                    f"`each_type_code_column`, " \
                                                    f"{self.group_by_all_columns_inner_select} " \
                                                    f"FROM " \
                                                    f"table_name) " \
                                                    f"GROUP BY " \
                                                    f"{self.group_by_all_columns_outer_select} " \
                                                    f"ORDER BY " \
                                                    f"{self.group_by_all_columns_outer_select} "
            self.queries_executed["check_type_code_count"] = counts_by_each_type_code_column_query
            self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                                 f"check_type_code_count method, "
                                                 f"counts_by_each_type_code_column_query --> "
                                                 f"{counts_by_each_type_code_column_query}")
            for each_type_code_column in self.type_code_columns_list:
                fin_counts_by_each_type_code_column_query = counts_by_each_type_code_column_query.replace(
                    "each_type_code_column", each_type_code_column
                )
                self.generic_process_date_wise(fin_counts_by_each_type_code_column_query, each_type_code_column)
        else:
            self.common_operations.log_and_print("Inside SparkCountCompareWithoutTempTable "
                                                 "check_type_code_count method, "
                                                 "len(type_code_columns_list) --> "
                                                 f"{len(self.type_code_columns_list)} "
                                                 f"IS ZERO OR "
                                                 f"group_by_all_columns_outer_select --> "
                                                 f"{self.group_by_all_columns_outer_select} OR "
                                                 f"group_by_all_columns_inner_select --> "
                                                 f"{self.group_by_all_columns_inner_select}, "
                                                 f"IS EMPTY")

    def check_amount_count(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_amount_count method **********")
        if len(self.amount_columns_list) != 0 and \
                self.group_by_all_columns_outer_select and \
                self.group_by_all_columns_inner_select:
            counts_by_each_amount_column_query = f"SELECT " \
                                                 f"COALESCE(sum(`each_amount_column`), 0) " \
                                                 f"as distinct_count_of_records, " \
                                                 f"COALESCE(sum(`each_amount_column`), 0) " \
                                                 f"as count_of_records, " \
                                                 f"{self.group_by_all_columns_outer_select} " \
                                                 f"FROM " \
                                                 f"(SELECT " \
                                                 f"`each_amount_column`, " \
                                                 f"{self.group_by_all_columns_inner_select} " \
                                                 f"FROM " \
                                                 f"table_name) " \
                                                 f"GROUP BY " \
                                                 f"{self.group_by_all_columns_outer_select} " \
                                                 f"ORDER BY " \
                                                 f"{self.group_by_all_columns_outer_select} "
            self.queries_executed["check_amount_count"] = counts_by_each_amount_column_query
            self.common_operations.log_and_print(f"Inside SparkCountCompareWithoutTempTable "
                                                 f"check_amount_count method, "
                                                 f"counts_by_each_amount_column_query --> "
                                                 f"{counts_by_each_amount_column_query}")
            for each_amount_column in self.amount_columns_list:
                fin_counts_by_each_amount_column_query = counts_by_each_amount_column_query.replace(
                    "each_amount_column", each_amount_column
                )
                self.generic_process_date_wise(fin_counts_by_each_amount_column_query, each_amount_column)
        else:
            self.common_operations.log_and_print("Inside SparkCountCompareWithoutTempTable "
                                                 "check_amount_count method, "
                                                 "len(amount_columns_list) --> "
                                                 f"{len(self.amount_columns_list)} "
                                                 f"IS ZERO OR "
                                                 f"group_by_all_columns_outer_select --> "
                                                 f"{self.group_by_all_columns_outer_select} OR "
                                                 f"group_by_all_columns_inner_select --> "
                                                 f"{self.group_by_all_columns_inner_select}, "
                                                 f"IS EMPTY")

    def form_count_summary_dict(self, passed_attribute_name, passed_attribute_value):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "form_count_summary_dict method **********")
        super().form_count_summary_dict(passed_attribute_name, passed_attribute_value)

    def check_summary(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "check_summary method **********")
        super().check_summary()

    def run_counts_compare(self):
        self.common_operations.log_and_print("********** Inside SparkCountCompareWithoutTempTable "
                                             "run_counts_compare method **********")
        super().run_counts_compare()
