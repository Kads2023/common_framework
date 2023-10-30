import os
import json
import pandas as pd

from validation_framework.common_validate.endpoints.end_point import EndPoint
from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils import global_constants
from validation_framework.common_validate.utils.keymaker_utils import KeyMakerApiProxy
from validation_framework.common_validate.utils.common_utils import CommonOperations


class SFEndpoint(EndPoint):
    no_of_gcs_files = 0
    __config = None
    __all_ignore_columns = []

    __user = '<>'
    __account = '<>'
    __password = '<>'
    __role = '<>'
    __warehouse = 'LOAD_WH'
    __port = '443'
    __limit_statement = ''

    def __init__(self, passed_job_params: JobParam,
                 passed_common_operations: CommonOperations,
                 passed_keymaker: KeyMakerApiProxy,
                 passed_endpoint_name: str,
                 passed_endpoint_type: str):
        self.__job_params = passed_job_params
        self.__common_operations = passed_common_operations
        self.__key_maker = passed_keymaker
        self.__endpoint_name = passed_endpoint_name.strip().upper()
        self.__endpoint_type = passed_endpoint_type.strip().lower()
        self.__common_operations. \
            log_and_print("********** In __init__ of SFEndpoint ********** ")
        self.__common_operations. \
            log_and_print("In __init__ of SFEndpoint, "
                          f"endpoint_name --> {self.__endpoint_name}, "
                          f"endpoint_type --> {self.__endpoint_type} ")
        self.__base_path = self.__job_params.get_params(global_constants.base_path_key_name)
        self.__xp_base_path = self.__job_params.get_params(global_constants.xp_base_path_key_name)
        self.__common_base_path = self.__job_params.get_params(global_constants.common_base_path_key_name)

        self.__limit_sample_data = self.__job_params.safe_get_params(
            global_constants.common_section + ",LIMIT_SAMPLE_DATA"
        )

        self.__now_base_validation_type_keys, self.__now_validation_type_keys = self.\
            __common_operations.get_validation_type_keys(self.__endpoint_name, self.__endpoint_type)

        now_http_proxy = self.__now_base_validation_type_keys.get("HTTP_PROXY", "")
        if now_http_proxy:
            self.__http_proxy = now_http_proxy
        else:
            error_msg = "In __init__ of SFEndpoint, " \
                        f"endpoint_name --> {self.__endpoint_name}, " \
                        f"endpoint_type --> {self.__endpoint_type}, " \
                        f"now_base_validation_type_keys --> {self.__now_base_validation_type_keys}, " \
                        f"now_validation_type_keys --> {self.__now_validation_type_keys}, " \
                        f"HTTP_PROXY not found or empty"
            self.__common_operations.raise_value_error(error_msg)

        now_https_proxy = self.__now_base_validation_type_keys.get("HTTPS_PROXY", "")
        if now_https_proxy:
            self.__https_proxy = now_https_proxy
        else:
            error_msg = "In __init__ of SFEndpoint, " \
                        f"endpoint_name --> {self.__endpoint_name}, " \
                        f"endpoint_type --> {self.__endpoint_type}, " \
                        f"now_base_validation_type_keys --> {self.__now_base_validation_type_keys}, " \
                        f"now_validation_type_keys --> {self.__now_validation_type_keys}, " \
                        f"HTTPS_PROXY not found or empty"
            self.__common_operations.raise_value_error(error_msg)

        self.__datatype_transform = self.__now_base_validation_type_keys.get("DATATYPE_TRANSFORM", {})
        self.__datatype_transform_keys = list(self.__datatype_transform.keys())

        self.__special_datatype_transform = self.__now_base_validation_type_keys.get("SPECIAL_DATATYPE_TRANSFORM", {})
        self.__special_datatype_transform_keys = list(self.__special_datatype_transform.keys())

        now_convert_column_names_to_lower_case = \
            self.__now_base_validation_type_keys.get("CONVERT_COLUMN_NAMES_TO_LOWER_CASE", "")
        if now_convert_column_names_to_lower_case:
            self.__convert_column_names_to_lower_case = now_convert_column_names_to_lower_case
        else:
            self.__convert_column_names_to_lower_case = global_constants.default_convert_column_names_to_lower_case

        self.__config_limit_sample = \
            self.__now_base_validation_type_keys.get("LIMIT_SAMPLE", "")

        self.__job_level_now_ignore_columns = \
            self.__now_base_validation_type_keys.get("IGNORE_COLUMNS", [])
        self.__all_ignore_columns.extend(self.__job_level_now_ignore_columns)
        self.__using_spark = self.__job_params.get_params("using_spark")
        self.__display_data = self.__common_operations.safe_get_params(
            global_constants.common_section + ",DISPLAY_DATA"
        )
        self.__display_credentials = self.__job_params.get_params("display_credentials")
        self.__validation_key_name = str(
            self.__job_params.get_params("validation_key_name")
        ).strip().lower()
        self.__validation_date = str(
            self.__job_params.get_params("validation_date")
        ).strip()

        self.__validation_key_name_keys = self.__job_params.get_params(self.__validation_key_name)
        self.__end_point_type_keys = self.__validation_key_name_keys.get(
            f"{self.__endpoint_type.upper()}_DETAILS", {})

        self.__common_operations. \
            log_and_print("In __init__ of SFEndpoint, "
                          f"validation_key_name_keys --> {self.__validation_key_name_keys}, "
                          f"end_point_type_keys --> {self.__end_point_type_keys}, "
                          f"all_ignore_columns --> {self.__all_ignore_columns}")

        self.__display_string = f"endpoint_type --> {self.__endpoint_type}, " \
                                f"display_credentials --> {self.__display_credentials}"

        if self.__display_credentials == "True":
            self.__credentials_display_string = f"USER --> " \
                                                f"{self.__user}, " \
                                                f"ACCOUNT --> {self.__account}, " \
                                                f"ROLE --> {self.__role}, " \
                                                f"WAREHOUSE --> {self.__warehouse}, " \
                                                f"PORT --> {self.__port}"
            self.__display_string = self.__common_operations.append_string(
                self.__display_string, self.__credentials_display_string
            )

        self.__full_table_name = \
            self.__end_point_type_keys.get("FULL_TABLE_NAME", "")

        if self.__full_table_name:
            table_details = str(self.__full_table_name).strip().split(".")

            now_sf_database = \
                self.__end_point_type_keys.get("DATABASE", "")
            if now_sf_database:
                self.__sf_database = now_sf_database
            else:
                self.__sf_database = table_details[0].strip()

            now_sf_schema = \
                self.__end_point_type_keys.get("SCHEMA", "")
            if now_sf_schema:
                self.__sf_schema = now_sf_schema
            else:
                self.__sf_schema = table_details[1].strip()

            now_table_name = \
                self.__end_point_type_keys.get("TABLE_NAME", "")
            if now_table_name:
                self.__table_name = now_table_name
            else:
                self.__table_name = table_details[2].strip()

            if self.__limit_sample_data == "True":
                validation_key_limit_sample = \
                    self.__validation_key_name_keys.get("LIMIT_SAMPLE", "")
                end_point_limit_sample = \
                    self.__end_point_type_keys.get("LIMIT_SAMPLE", "")
                if validation_key_limit_sample:
                    self.__limit_sample = validation_key_limit_sample
                elif end_point_limit_sample:
                    self.__limit_sample = end_point_limit_sample
                elif self.__config_limit_sample:
                    self.__limit_sample = self.__config_limit_sample
                else:
                    self.__limit_sample = global_constants.default_limit_sample

                if self.__limit_sample:
                    self.__limit_statement = f"LIMIT {self.__limit_sample}"

            now_where_clause = \
                self.__validation_key_name_keys.get("WHERE_CLAUSE", "")
            if now_where_clause:
                final_where_clause = now_where_clause
            else:
                final_where_clause = \
                    self.__end_point_type_keys.get("WHERE_CLAUSE", "")

            self.__where_clause = final_where_clause.replace(
                "validation_date",
                self.__validation_date
            )

            self.__run_for_whole_table = \
                self.__validation_key_name_keys.get("RUN_FOR_WHOLE_TABLE", "")

            now_query = \
                self.__validation_key_name_keys.get("QUERY", "")
            if now_query:
                self.__query = now_query
            else:
                self.__query = \
                    self.__end_point_type_keys.get("QUERY", "")

            self.__table_schema = self.get_table_schema()
            self.__table_columns = list(self.__table_schema.keys())

            now_select_columns = \
                self.__validation_key_name_keys.get("SELECT_COLUMNS", [])
            if not now_select_columns:
                now_select_columns = \
                    self.__end_point_type_keys.get("SELECT_COLUMNS", [])

            self.__select_columns = []
            for each_select_column in now_select_columns:
                self.__select_columns.append(f"{each_select_column}")

            now_special_columns = \
                self.__validation_key_name_keys.get("SPECIAL_COLUMNS", {})
            if now_special_columns:
                self.__special_columns = now_special_columns
            else:
                self.__special_columns = \
                    self.__end_point_type_keys.get("SPECIAL_COLUMNS", {})

            self.__special_columns_keys = list(self.__special_columns.keys())

            self.__common_operations.log_and_print(f"In __init__ of SFEndpoint, "
                                                   f"table_columns --> {self.__table_columns}, "
                                                   f"table_schema --> {self.__table_schema}, "
                                                   f"special_columns_keys --> {self.__special_columns_keys}, "
                                                   f"special_columns --> {self.__special_columns}, "
                                                   f"special_datatype_transform_keys --> "
                                                   f"{self.__special_datatype_transform_keys}, "
                                                   f"special_datatype_transform --> "
                                                   f"{self.__special_datatype_transform}, "
                                                   f"datatype_transform_keys --> "
                                                   f"{self.__datatype_transform_keys}, "
                                                   f"datatype_transform --> {self.__datatype_transform}")

            if len(self.__select_columns) > 0:
                self.__final_select_columns = ', '.join(self.__select_columns)
            else:
                if self.__convert_column_names_to_lower_case == "True":
                    for each_column in self.__table_columns:
                        each_column_data_type = str(self.__table_schema[each_column]
                                                    ).strip().upper()
                        if each_column in self.__special_columns_keys:
                            column_value_str = f"{self.__special_columns[each_column]}"
                            append_str = f"{column_value_str} AS {str(each_column).lower()}"
                        else:
                            if each_column_data_type in self.__special_datatype_transform_keys:
                                column_value_str = str(
                                    self.__special_datatype_transform[each_column_data_type]).strip().replace(
                                    "column_name", each_column
                                )
                                append_str = f"{column_value_str} AS {str(each_column).lower()}"
                            elif each_column_data_type in self.__datatype_transform_keys:
                                append_str = f"CAST({each_column} AS " \
                                             f"{self.__datatype_transform[each_column_data_type]}" \
                                             f") AS {str(each_column).lower()}"
                            else:
                                append_str = f"{each_column} AS {str(each_column).lower()}"
                        self.__common_operations.log_and_print(f"In __init__ of SFEndpoint, "
                                                               f"append_str --> {append_str}")
                        self.__select_columns.append(append_str)
                    self.__final_select_columns = ', '.join(self.__select_columns)
                else:
                    self.__final_select_columns = '*'

            now_ignore_columns = \
                self.__validation_key_name_keys.get("IGNORE_COLUMNS", [])
            if now_ignore_columns:
                self.__ignore_columns = now_ignore_columns
            else:
                self.__ignore_columns = \
                    self.__end_point_type_keys.get("IGNORE_COLUMNS", [])
            self.__common_operations. \
                log_and_print("In __init__ of SFEndpoint, "
                              f"all_ignore_columns --> {self.__all_ignore_columns}")
        else:
            if self.__endpoint_type == "results":
                self.__common_operations. \
                    log_and_print("In __init__ of SFEndpoint, "
                                  f"endpoint_name --> {self.__endpoint_name}, "
                                  f"endpoint_type --> {self.__endpoint_type} IS results, "
                                  f"HENCE FULL_TABLE_NAME IS NOT MANDATORY")
            else:
                error_msg = "In __init__ of SFEndpoint, " \
                            f"endpoint_name --> {self.__endpoint_name}, " \
                            f"endpoint_type --> {self.__endpoint_type}, " \
                            f"end_point_type_keys --> {self.__end_point_type_keys}, " \
                            f"FULL_TABLE_NAME not found or empty"
                self.__common_operations.raise_value_error(error_msg)

        self.__run_no = 0

        self.__sf_cursor = self.get_sf_cursor()
        self.__common_operations. \
            log_and_print("In __init__ of SFEndpoint, created sf_cursor")

    def set_proxy(self):
        self.__common_operations. \
            log_and_print("********** In set_proxy of SFEndpoint ********** ")
        os.environ["HTTP_PROXY"] = self.__http_proxy
        os.environ["HTTPS_PROXY"] = self.__https_proxy

    def get_sf_cursor(self):
        self.__common_operations. \
            log_and_print("********** In get_sf_cursor of SFEndpoint ********** ")
        self.set_proxy()
        import snowflake.connector
        con = snowflake.connector.connect(
            user=self.__user,
            account=self.__account,
            password=self.__password,
            role=self.__role,
            warehouse=self.__warehouse,
            database=self.__sf_database,
            schema=self.__sf_schema,
            port=self.__port
        )
        new_cursor = con.cursor()
        return new_cursor

    def check_table(self):
        self.__common_operations. \
            log_and_print("********** In check_table "
                          "of SFEndpoint ********** ")
        if len(self.__table_schema) != 0:
            return True
        else:
            return False

    def get_table_schema(self,
                         passed_labels=None,
                         lower_column_names="False"):
        self.__common_operations. \
            log_and_print("********** In get_table_schema "
                          "of SFEndpoint ********** ")
        self.__common_operations. \
            log_and_print(f"In get_table_schema of SFEndpoint, "
                          f"passed_labels --> {passed_labels}, "
                          f"lower_column_names --> {lower_column_names}, "
                          f"{self.__display_string}, ")
        self.set_proxy()
        import snowflake.connector
        con = snowflake.connector.connect(
            user=self.__user,
            account=self.__account,
            password=self.__password,
            role=self.__role,
            warehouse=self.__warehouse,
            database=self.__sf_database,
            schema='INFORMATION_SCHEMA',
            port=self.__port
        )
        new_cursor = con.cursor()
        schema_query = f"SELECT COLUMN_NAME, DATA_TYPE FROM COLUMNS " \
                       f"WHERE " \
                       f"TABLE_CATALOG = '{self.__sf_database}' AND " \
                       f"TABLE_SCHEMA = '{self.__sf_schema}' AND " \
                       f"TABLE_NAME = '{self.__table_name}' " \
                       f"ORDER BY ORDINAL_POSITION"
        new_cursor.execute(schema_query)
        table_schema = {}
        for (now_column_name, now_data_type) in new_cursor:
            if now_column_name not in self.__all_ignore_columns:
                if lower_column_names == "True":
                    lower_column_name = now_column_name.strip().lower()
                    if lower_column_name not in self.__all_ignore_columns:
                        table_schema[lower_column_name] = now_data_type
                else:
                    table_schema[now_column_name] = now_data_type
        new_cursor.close()
        return table_schema

    def get_table_data(self,
                       passed_where_connector="AND",
                       passed_labels=None):
        self.__common_operations. \
            log_and_print("********** In get_table_data"
                          " of SFEndpoint ********** ")
        additional_where_clause = self.__common_operations.safe_get_params("additional_where_clause")
        self.__common_operations. \
            log_and_print("In get_table_data of SFEndpoint, "
                          f"{self.__display_string}, "
                          "where_clause --> "
                          f"{self.__where_clause}, "
                          f"additional_where_clause --> "
                          f"{additional_where_clause}, "
                          f"passed_labels --> {passed_labels}")
        final_query = ""
        final_where_clause = ""
        if self.__query or self.__run_for_whole_table == "True":
            if additional_where_clause:
                final_where_clause = f"WHERE " \
                                     f"{additional_where_clause}"
        elif self.__where_clause:
            if additional_where_clause:
                final_where_clause = f"WHERE {self.__where_clause} " \
                                     f"{passed_where_connector} " \
                                     f"{additional_where_clause}"
            else:
                final_where_clause = f"WHERE {self.__where_clause} "

        if self.__where_clause:
            final_query = f"SELECT {self.__final_select_columns} " \
                          f"FROM {self.__full_table_name} " \
                          f"{final_where_clause} " \
                          f"{self.__limit_statement}"
        elif self.__run_for_whole_table == "True":
            final_query = f"SELECT {self.__final_select_columns} " \
                          f"FROM {self.__full_table_name} " \
                          f"{final_where_clause} {self.__limit_statement}"
        elif self.__query:
            final_query = f"{self.__query} "
            now_query = str(self.__query).strip().lower()
            if now_query.count("limit ") == 0 \
                    and now_query.count("group by") == 0 \
                    and now_query.count("order by") == 0:
                if now_query.count("where") == 1:
                    if additional_where_clause:
                        final_query = f"{self.__query} " \
                                      f"{passed_where_connector} {additional_where_clause} " \
                                      f"{self.__limit_statement}"
                else:
                    final_query = f"{self.__query} " \
                                  f"{final_where_clause} " \
                                  f"{self.__limit_statement}"
        else:
            error_msg = f"In get_table_data of SFEndpoint, " \
                        f"where_clause --> {self.__where_clause}, " \
                        f"run_for_whole_table --> {self.__run_for_whole_table}, " \
                        f"query --> {self.__query}, " \
                        f"EITHER where_clause OR query OR run_for_whole_table, " \
                        f"NEEDS TO BE PROVIDED"
            self.__common_operations.raise_value_error(error_msg)
        self.__common_operations. \
            log_and_print(f"In get_table_data of SFEndpoint, "
                          f"{self.__display_string}, "
                          f"full_table_name --> {self.__full_table_name}, "
                          f"final_where_clause --> {final_where_clause}, "
                          f"limit_statement --> {self.__limit_statement}, "
                          f"query --> {self.__query}, "
                          f"final_query --> {final_query}, "
                          f"all_ignore_columns --> {self.__all_ignore_columns}")
        self.__common_operations.call_set_job_details_params(f"{self.__endpoint_type}_query",
                                                             final_query, _check=True)
        self.__common_operations.call_set_job_details_params(f"{self.__endpoint_type}_limit",
                                                             self.__limit_statement, _check=True)
        self.set_proxy()
        self.__sf_cursor.execute(final_query.replace('`', ''))
        data_frame = self.__sf_cursor.fetch_pandas_all()
        count_value = len(data_frame)

        self.__common_operations. \
            log_and_print(f"In get_table_data of SFEndpoint, "
                          f"using_spark --> {self.__using_spark}, "
                          f"count_value --> {count_value}, "
                          f"Got data_frame :: {data_frame}")
        if self.__all_ignore_columns:
            final_ignore_columns = list(set(self.__all_ignore_columns))
            final_data_frame = data_frame.drop(*final_ignore_columns)
        else:
            final_data_frame = data_frame
        count_value = len(final_data_frame)

        self.__common_operations. \
            log_and_print(f"In get_table_data of SFEndpoint, "
                          f"convert_column_names_to_lower_case --> {self.__convert_column_names_to_lower_case}, "
                          f"actual_data_frame :: {final_data_frame}")

        if self.__convert_column_names_to_lower_case == "True":
            columns_lowered_data_frame = final_data_frame.rename(columns=str.lower)
        else:
            columns_lowered_data_frame = final_data_frame

        self.__common_operations. \
            log_and_print(f"In get_table_data of SFEndpoint, "
                          f"convert_column_names_to_lower_case --> {self.__convert_column_names_to_lower_case}, "
                          f"columns_lowered_data_frame :: {columns_lowered_data_frame}")
        if self.__using_spark == "True":
            pandas_df_columns = columns_lowered_data_frame.columns
            pandas_df_dtypes = columns_lowered_data_frame.dtypes
            self.__common_operations. \
                log_and_print(f"In get_table_data of SFEndpoint, "
                              f"using_spark --> {self.__using_spark}, "
                              f"pandas_df_columns --> {pandas_df_columns}, "
                              f"pandas_df_dtypes --> {pandas_df_dtypes}")
            spark_schema = self.__common_operations.get_pandas_df_schema(
                list(pandas_df_columns), list(pandas_df_dtypes)
            )
            spark = self.__job_params.spark_session
            return_data_frame = spark.createDataFrame(columns_lowered_data_frame, schema=spark_schema)
            spark_count_value = return_data_frame.count()
            self.__common_operations. \
                log_and_print(f"In get_table_data of SFEndpoint, "
                              f"using_spark --> {self.__using_spark}, "
                              f"spark_count_value --> {spark_count_value}")
            self.__common_operations.display_data_frame(return_data_frame,
                                                        f"{self.__endpoint_type}_DATAFRAME")
        else:
            return_data_frame = columns_lowered_data_frame

        self.__common_operations. \
            log_and_print(f"In get_table_data of SFEndpoint, "
                          f"count_value --> {count_value}, "
                          f"Got final_data_frame :: {final_data_frame}")

        return return_data_frame

    def execute_query_and_get_counts(self,
                                     passed_query,
                                     query_type,
                                     fetched_value_name,
                                     passed_labels=None):
        self.__common_operations. \
            log_and_print("********** In execute_query_and_get_counts"
                          " of SFEndpoint ********** ")
        self.__common_operations. \
            log_and_print(f"In execute_query_and_get_counts of SFEndpoint, "
                          f"passed_labels --> {passed_labels}, "
                          f"{self.__display_string}, "
                          f"query_type --> {query_type}, "
                          f"fetched_value_name --> {fetched_value_name}, "
                          f"passed_query --> {passed_query}")
        self.set_proxy()
        self.__sf_cursor.execute(passed_query.replace('`', ''))
        ret_val = ""
        for now_value in self.__sf_cursor:
            ret_val = str(now_value).replace("(", "").replace(",)", "")
        self.__common_operations. \
            log_and_print(f"Got results :: {ret_val}, "
                          f"for query --> {passed_query}")
        return ret_val

    def execute_query_and_get_data_frame(self,
                                         passed_query,
                                         query_type,
                                         passed_labels=None):
        self.__common_operations. \
            log_and_print("********** In execute_query_and_get_data_frame"
                          " of SFEndpoint ********** ")
        self.__common_operations. \
            log_and_print(f"In execute_query_and_get_data_frame of SFEndpoint, "
                          f"{self.__display_string}, "
                          f"query_type --> {query_type}, "
                          f"passed_query --> {passed_query}, "
                          f"passed_labels --> {passed_labels}")
        self.set_proxy()
        self.__sf_cursor.execute(passed_query.replace('`', ''))
        final_return_data = self.__sf_cursor.fetch_pandas_all()
        count_value = len(final_return_data)

        if self.__using_spark == "True":
            pandas_df_columns = final_return_data.columns
            pandas_df_dtypes = final_return_data.dtypes
            self.__common_operations. \
                log_and_print(f"In execute_query_and_get_data_frame of SFEndpoint, "
                              f"using_spark --> {self.__using_spark}, "
                              f"pandas_df_columns --> {pandas_df_columns}, "
                              f"pandas_df_dtypes --> {pandas_df_dtypes}")
            spark_schema = self.__common_operations.get_pandas_df_schema(
                list(pandas_df_columns), list(pandas_df_dtypes)
            )
            spark = self.__job_params.spark_session
            return_data_frame = spark.createDataFrame(final_return_data, schema=spark_schema)
            spark_count_value = return_data_frame.count()
            self.__common_operations. \
                log_and_print(f"In execute_query_and_get_data_frame of SFEndpoint, "
                              f"using_spark --> {self.__using_spark}, "
                              f"spark_count_value --> {spark_count_value}")
            self.__common_operations.display_data_frame(return_data_frame,
                                                        f"{self.__endpoint_type}_{query_type}_DATAFRAME")
        else:
            return_data_frame = final_return_data

        self.__common_operations. \
            log_and_print(f"In execute_query_and_get_data_frame of SFEndpoint, "
                          f"count_value --> {count_value}, "
                          f"Got results :: {return_data_frame}")
        return return_data_frame

    def close_connection(self):
        self.__common_operations. \
            log_and_print("********** In close_connection"
                          " of SFEndpoint ********** ")
        if self.__sf_cursor:
            self.set_proxy()
            self.__sf_cursor.close()
