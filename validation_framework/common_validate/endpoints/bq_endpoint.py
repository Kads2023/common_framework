import os
import json

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from validation_framework.common_validate.endpoints.end_point import EndPoint
from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils import global_constants
from validation_framework.common_validate.utils.keymaker_utils import KeyMakerApiProxy
from validation_framework.common_validate.utils.common_utils import CommonOperations

import pandas as pd


class BQEndpoint(EndPoint):
    no_of_gcs_files = 0
    __config = None
    __run_no = 0
    __bq_project = None
    __all_ignore_columns = []
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
            log_and_print("********** In __init__ of BQEndpoint ********** ")
        self.__common_operations. \
            log_and_print("In __init__ of BQEndpoint, "
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

        self.__identity = self.__now_base_validation_type_keys["IDENTITY"].\
            format(base_path=self.__base_path,
                   common_base_path=self.__common_base_path,
                   xp_base_path=self.__xp_base_path)
        self.__credentials_json = self.__now_base_validation_type_keys["KM_CREDENTIALS_JSON"]
        self.__credentials_json_label = self.__now_base_validation_type_keys.get(
            "KM_CREDENTIALS_JSON_LABEL",
            self.__credentials_json
        )
        self.km_endpoint = self.__job_params.get_params(global_constants.common_section +
                                                        "," + global_constants.km_end_point)
        keymaker_response = self.__key_maker.get_keymaker_api_response(self.km_endpoint, self.__identity)
        gcs_credential_json = self.__key_maker.get_keymaker_key(keymaker_response, self.__credentials_json)
        gcs_credential_json_type = type(gcs_credential_json)
        try:
            self.__gcs_credential_json = json.loads(gcs_credential_json)
        except TypeError as te:
            self.__common_operations. \
                log_and_print("In __init__ of BQEndpoint, "
                              f"gcs_credential_json_type --> {gcs_credential_json_type}, "
                              f"TypeError --> {te}")
            self.__gcs_credential_json = gcs_credential_json

        self.__bq_client = self.get_bq_client()
        self.__common_operations. \
            log_and_print("In __init__ of BQEndpoint, created bq_client, "
                          f"credentials_json_label --> {self.__credentials_json_label}")

        now_gcs_project = self.__now_base_validation_type_keys.get("GCS_PROJECT", "")
        if now_gcs_project:
            self.__gcs_project = now_gcs_project
        else:
            error_msg = "In __init__ of BQEndpoint, " \
                        f"endpoint_name --> {self.__endpoint_name}, " \
                        f"endpoint_type --> {self.__endpoint_type}, " \
                        f"now_base_validation_type_keys --> {self.__now_base_validation_type_keys}, " \
                        f"now_validation_type_keys --> {self.__now_validation_type_keys}, " \
                        f"GCS_PROJECT not found or empty"
            self.__common_operations.raise_value_error(error_msg)

        now_parent_project = self.__now_validation_type_keys.get("PARENT_PROJECT", "")
        if now_parent_project:
            self.__parent_project = now_parent_project
        else:
            self.__parent_project = self.__gcs_project

        self.__common_operations. \
            log_and_print("In __init__ of BQEndpoint, "
                          f"gcs_credential_json --> {self.__gcs_credential_json}, "
                          f"gcs_project --> {self.__gcs_project}, "
                          f"parent_project --> {self.__parent_project}")

        materialization_dataset = self.__now_base_validation_type_keys.get("MATERIALIZATION_DATASET", "")
        if materialization_dataset:
            self.__materialization_dataset = materialization_dataset
        else:
            error_msg = "In __init__ of BQEndpoint, " \
                        f"endpoint_name --> {self.__endpoint_name}, " \
                        f"endpoint_type --> {self.__endpoint_type}, " \
                        f"now_base_validation_type_keys --> {self.__now_base_validation_type_keys}, " \
                        f"now_validation_type_keys --> {self.__now_validation_type_keys}, " \
                        f"MATERIALIZATION_DATASET not found or empty"
            self.__common_operations.raise_value_error(error_msg)

        now_temp_gcs_bucket_name = self.__now_base_validation_type_keys.get("TEMP_GCS_BUCKET_NAME", "")
        if now_temp_gcs_bucket_name:
            self.__temp_gcs_bucket_name = now_temp_gcs_bucket_name
        else:
            error_msg = "In __init__ of BQEndpoint, " \
                        f"endpoint_name --> {self.__endpoint_name}, " \
                        f"endpoint_type --> {self.__endpoint_type}, " \
                        f"now_base_validation_type_keys --> {self.__now_base_validation_type_keys}, " \
                        f"now_validation_type_keys --> {self.__now_validation_type_keys}, " \
                        f"TEMP_GCS_BUCKET_NAME not found or empty"
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
            log_and_print("In __init__ of BQEndpoint, "
                          f"validation_key_name_keys --> {self.__validation_key_name_keys}, "
                          f"end_point_type_keys --> {self.__end_point_type_keys}, "
                          f"all_ignore_columns --> {self.__all_ignore_columns}")

        self.__display_string = f"using_spark --> {self.__using_spark}, " \
                                f"display_data --> {self.__display_data}, " \
                                f"endpoint_type --> {self.__endpoint_type}, " \
                                f"display_credentials --> {self.__display_credentials}"

        if self.__display_credentials == "True":
            self.__credentials_display_string = f"gcs_credential_json --> " \
                                                f"{self.__gcs_credential_json}, " \
                                                f"gcs_project --> {self.__gcs_project}, " \
                                                f"parent_project --> {self.__parent_project}"
            self.__display_string = self.__common_operations.append_string(
                self.__display_string, self.__credentials_display_string
            )

        self.__full_table_name = \
            self.__end_point_type_keys.get("FULL_TABLE_NAME", "")

        if self.__full_table_name:
            table_details = str(self.__full_table_name).strip().split(".")

            now_bq_dataset = \
                self.__end_point_type_keys.get("DATASET", "")
            if now_bq_dataset:
                self.__bq_dataset = now_bq_dataset
            else:
                self.__bq_dataset = table_details[1].strip()

            now_bq_project = \
                self.__end_point_type_keys.get("BQ_PROJECT", "")
            if now_bq_project:
                self.__bq_project = now_bq_project
            else:
                self.__bq_project = table_details[0].strip()

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
                self.__select_columns.append(f"`{each_select_column}`")

            now_special_columns = \
                self.__validation_key_name_keys.get("SPECIAL_COLUMNS", {})
            if now_special_columns:
                self.__special_columns = now_special_columns
            else:
                self.__special_columns = \
                    self.__end_point_type_keys.get("SPECIAL_COLUMNS", {})

            self.__special_columns_keys = list(self.__special_columns.keys())

            self.__common_operations.log_and_print(f"In __init__ of BQEndpoint, "
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
                        self.__common_operations.log_and_print(f"In __init__ of BQEndpoint, "
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
            self.__all_ignore_columns.extend(self.__ignore_columns)
            self.__common_operations. \
                log_and_print("In __init__ of BQEndpoint, "
                              f"all_ignore_columns --> {self.__all_ignore_columns}")
        else:
            if self.__endpoint_type == "results":
                self.__common_operations. \
                    log_and_print("In __init__ of BQEndpoint, "
                                  f"endpoint_name --> {self.__endpoint_name}, "
                                  f"endpoint_type --> {self.__endpoint_type} IS results, "
                                  f"HENCE FULL_TABLE_NAME IS NOT MANDATORY")
            else:
                error_msg = "In __init__ of BQEndpoint, " \
                            f"endpoint_name --> {self.__endpoint_name}, " \
                            f"endpoint_type --> {self.__endpoint_type}, " \
                            f"end_point_type_keys --> {self.__end_point_type_keys}, " \
                            f"FULL_TABLE_NAME not found or empty"
                self.__common_operations.raise_value_error(error_msg)

        materialization_project = self.__now_base_validation_type_keys.get("MATERIALIZATION_PROJECT", "")
        if materialization_project:
            self.__materialization_project = materialization_project
        else:
            if self.__bq_project:
                self.__materialization_project = self.__bq_project
            else:
                self.__materialization_project = None

    def reset_proxy(self):
        self.__common_operations. \
            log_and_print("********** In reset_proxy of BQEndpoint ********** ")
        try:
            del os.environ["HTTP_PROXY"]
        except KeyError as e:
            self.__common_operations.log_and_print("In reset_proxy of BQEndpoint, "
                                                   f"error_msg --> {e}")
        try:
            del os.environ["HTTPS_PROXY"]
        except KeyError as e:
            self.__common_operations.log_and_print("In reset_proxy of BQEndpoint, "
                                                   f"error_msg --> {e}")

    def get_bq_job_labels(self):
        self.__common_operations. \
            log_and_print("********** In get_bq_job_labels of BQEndpoint ********** ")
        self.__common_operations. \
            log_and_print("In get_bq_job_labels of BQEndpoint, "
                          f"endpoint_type --> {self.__endpoint_type}")
        tag = str(self.__job_params.get_params(global_constants.common_section.strip() + ",TAG")).lower()
        validation_type = str(self.__job_params.get_params("validation_type")).strip().upper()
        km_app_name = str(self.__job_params.get_params(global_constants.common_section.strip() + ",KM_APP_NAME")).lower()
        end_run_id = global_constants.run_id
        label = str(self.__job_params.get_params(f"{validation_type},LABEL")).lower()
        target_table = str(self.__table_name).lower()
        dataset = str(self.__bq_dataset).lower()
        short_table_name = [word[:2] for word in target_table.split('_')]
        short_table_name = ''.join(short_table_name).replace("_", "").lower()
        short_table_name_new = [word[:1] for word in target_table.split('_')]
        short_table_name_new = ''.join(short_table_name_new).replace("_", "").lower()
        self.__run_no += 1
        ret_val = {
            "job-instance-id": "%s-%s-%s-%s-%s" % (tag, label[0],
                                                   short_table_name_new, end_run_id[2:], self.__run_no),
            "table_name": "%s-%s" % (tag, target_table),
            "property": "%s" % tag,
            "task": "%s-%s" % (tag, label),
            "dataset": "%s-%s" % (tag, dataset),
            "pp-edp-resource-name": "%s-%s-%s" % (tag, dataset, short_table_name),
            "pp-edp-custom-billing-tag": "%s-%s-%s-%s" % (tag, label, short_table_name, end_run_id[2:]),
            "pp-edp-job-run-id": "%s-%s-%s-%s-%s" % (tag, label[0],
                                                     short_table_name_new, end_run_id[2:], self.__run_no),
            "ingestion-time": "%s-%s" % (tag, global_constants.run_id),
            "pp-edp-platform": "non-opiniated",
            "pp-edp-business-unit": "edp-big-data-platform-engineering",
            "pp-edp-org-unit": "enterprise-data-solutions",
            "pp-edp-app-id": km_app_name,
            "pp-edp-app-name": km_app_name,
            "pp-edp-gcp-uid": self.__credentials_json_label
        }
        self.__common_operations. \
            log_and_print("In get_bq_job_labels of BQEndpoint, "
                          f"labels_now --> {ret_val}")
        return ret_val

    def get_bq_client(self):
        self.__common_operations. \
            log_and_print("********** In get_bq_client of BQEndpoint ********** ")
        self.reset_proxy()
        return bigquery.Client.from_service_account_info(self.__gcs_credential_json)

    def check_table(self):
        self.__common_operations. \
            log_and_print("********** In check_table "
                          "of BQEndpoint ********** ")
        self.__common_operations. \
            log_and_print("In check_table "
                          "of BQEndpoint, "
                          f"len(table_columns) --> "
                          f"{len(self.__table_columns)}")
        if len(self.__table_columns) > 0:
            return True
        else:
            return False

    def get_table_schema(self,
                         passed_labels=None,
                         lower_column_names="False"):
        self.__common_operations. \
            log_and_print("********** In get_table_schema of BQEndpoint ********** ")
        table_schema = {}
        query = f"SELECT * FROM " \
                f"{self.__bq_project}.{self.__bq_dataset}." \
                f"INFORMATION_SCHEMA.COLUMNS " \
                f"WHERE TABLE_NAME='{self.__table_name}'"
        self.__common_operations. \
            log_and_print(f"Inside get_table_schema of BQEndpoint, "
                          f"{self.__display_string}, "
                          f"Query to get bq table schema : {query}")

        # Setting query job labels
        if not passed_labels:
            passed_labels = self.get_bq_job_labels()
        job_config = bigquery.QueryJobConfig(labels=passed_labels)

        self.reset_proxy()
        query_job = self.__bq_client.query(
            query,
            project=self.__gcs_project,
            job_config=job_config
        )
        rows = query_job.result()
        for row in rows:
            now_column_name = str(row['column_name'])
            if now_column_name not in self.__all_ignore_columns:
                if lower_column_names == "True":
                    lower_column_name = now_column_name.strip().lower()
                    if lower_column_name not in self.__all_ignore_columns:
                        table_schema[lower_column_name] = row['data_type']
                else:
                    table_schema[now_column_name] = row['data_type']
        return table_schema

    def get_table_data(self,
                       passed_where_connector="AND",
                       passed_labels=None):
        self.__common_operations. \
            log_and_print("********** In get_table_data of BQEndpoint ********** ")
        additional_where_clause = self.__common_operations.safe_get_params("additional_where_clause")
        self.__common_operations. \
            log_and_print("In get_table_data of BQEndpoint, "
                          f"{self.__display_string}, "
                          "where_clause --> "
                          f"{self.__where_clause}, "
                          f"additional_where_clause --> "
                          f"{additional_where_clause}, "
                          f"passed_where_connector --> "
                          f"{passed_where_connector}, "
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
                          f"FROM `{self.__full_table_name}` " \
                          f"{final_where_clause} " \
                          f"{self.__limit_statement}"
        elif self.__run_for_whole_table == "True":
            final_query = f"SELECT {self.__final_select_columns} " \
                          f"FROM `{self.__full_table_name}` " \
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
            error_msg = f"In get_table_data of BQEndpoint, " \
                        f"where_clause --> {self.__where_clause}, " \
                        f"run_for_whole_table --> {self.__run_for_whole_table}, " \
                        f"query --> {self.__query}, " \
                        f"EITHER where_clause OR query OR run_for_whole_table, " \
                        f"NEEDS TO BE PROVIDED"
            self.__common_operations.raise_value_error(error_msg)
        self.__common_operations. \
            log_and_print(f"In get_table_data of BQEndpoint, "
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
        self.reset_proxy()
        if self.__using_spark == "True":
            spark = self.__job_params.spark_session
            spark.conf.set("viewsEnabled", "true")
            spark.conf.set("temporaryGcsBucket", self.__temp_gcs_bucket_name)
            spark.conf.set("materializationProject", self.__materialization_project)
            spark.conf.set("materializationDataset", self.__materialization_dataset)

            data_frame = spark.read.format("com.google.cloud.spark.bigquery") \
                .options(query=final_query,
                         credential=self.__gcs_credential_json,
                         project=self.__gcs_project,
                         parentProjectId=self.__parent_project) \
                .load() \
                .persist()
            count_value = data_frame.count()
        else:
            if not passed_labels:
                passed_labels = self.get_bq_job_labels()
            query_job_config = bigquery.QueryJobConfig(labels=passed_labels)
            data_frame = self.__bq_client.query(
                final_query,
                project=self.__gcs_project,
                job_config=query_job_config
            ).to_dataframe()
            count_value = len(data_frame)

        self.__common_operations.display_data_frame(data_frame,
                                                    f"{self.__endpoint_type}_DATAFRAME_BEFORE_DROP")
        if self.__all_ignore_columns:
            final_ignore_columns = list(set(self.__all_ignore_columns))
            final_data_frame = data_frame.drop(*final_ignore_columns)
        else:
            final_data_frame = data_frame

        self.__common_operations.display_data_frame(final_data_frame,
                                                    f"{self.__endpoint_type}_DATAFRAME_AFTER_DROP")
        self.__common_operations. \
            log_and_print(f"In get_table_data of BQEndpoint, "
                          f"using_spark --> {self.__using_spark}, "
                          f"count_value --> {count_value}")
        return final_data_frame

    def save_results(self, passed_data, passed_table_name):
        self.__common_operations. \
            log_and_print("********** In save_results of BQEndpoint ********** ")
        self.__common_operations. \
            log_and_print(f"In save_results of BQEndpoint, "
                          f"{self.__display_string}, "
                          f"passed_table_name --> {passed_table_name}")
        self.reset_proxy()
        if self.__using_spark == "True":
            passed_data.write.format("com.google.cloud.spark.bigquery").\
                mode("append")\
                .options(
                table=passed_table_name,
                credential=self.__gcs_credential_json,
                project=self.__gcs_project,
                parentProjectId=self.__parent_project).\
                save()
        else:
            load_job_config = bigquery.LoadJobConfig()
            load_job = self.__bq_client.load_table_from_dataframe(
                passed_data,
                passed_table_name,
                project=self.__gcs_project,
                job_config=load_job_config
            )
            now_results = load_job.result()
            self.__common_operations(f"In save_results of BQEndpoint, "
                                     f"now_results --> {now_results}")

    def execute_query_and_get_counts(self,
                                     passed_query,
                                     query_type,
                                     fetched_value_name,
                                     passed_labels=None):
        self.__common_operations. \
            log_and_print("********** In execute_query_and_get_counts"
                          " of BQEndpoint ********** ")
        self.__common_operations. \
            log_and_print(f"In execute_query_and_get_counts of BQEndpoint, "
                          f"{self.__display_string}, "
                          f"query_type --> {query_type}, "
                          f"fetched_value_name --> {fetched_value_name}, "
                          f"passed_query --> {passed_query}")
        final_return_data = None
        self.reset_proxy()
        if self.__using_spark == "True":
            spark = self.__job_params.spark_session
            spark.conf.set("viewsEnabled", "true")
            spark.conf.set("temporaryGcsBucket", self.__temp_gcs_bucket_name)
            spark.conf.set("materializationProject", self.__materialization_project)
            spark.conf.set("materializationDataset", self.__materialization_dataset)
            rows = spark.read.format("com.google.cloud.spark.bigquery")\
                .options(query=passed_query,
                         credential=self.__gcs_credential_json,
                         project=self.__gcs_project,
                         parentProjectId=self.__parent_project)\
                .load().collect()
        else:
            if not passed_labels:
                passed_labels = self.get_bq_job_labels()
            query_job_config = bigquery.QueryJobConfig(labels=passed_labels)
            query_job = self.__bq_client.query(
                passed_query,
                project=self.__gcs_project,
                job_config=query_job_config
            )
            rows = query_job.result()

        self.__common_operations. \
            log_and_print(f"In execute_query_and_get_counts of BQEndpoint, "
                          f"rows --> {rows}")
        for row in rows:
            final_return_data = str(row[fetched_value_name])

        self.__common_operations. \
            log_and_print(f"In execute_query_and_get_counts of BQEndpoint, "
                          f"using_spark --> {self.__using_spark}, "
                          f"fetched_value_name --> {fetched_value_name}, "
                          f"Got results :: {final_return_data}")
        return final_return_data

    def execute_query_and_get_data_frame(self,
                                         passed_query,
                                         query_type,
                                         passed_labels=None):
        self.__common_operations. \
            log_and_print("********** In execute_query_and_get_data_frame"
                          " of BQEndpoint ********** ")
        self.__common_operations. \
            log_and_print(f"In execute_query_and_get_data_frame of BQEndpoint, "
                          f"{self.__display_string}, "
                          f"query_type --> {query_type}, "
                          f"passed_query --> {passed_query}")
        self.reset_proxy()
        if self.__using_spark == "True":
            spark = self.__job_params.spark_session
            spark.conf.set("viewsEnabled", "true")
            spark.conf.set("temporaryGcsBucket", self.__temp_gcs_bucket_name)
            spark.conf.set("materializationProject", self.__materialization_project)
            spark.conf.set("materializationDataset", self.__materialization_dataset)
            final_return_data = spark.read.format("com.google.cloud.spark.bigquery")\
                .options(query=passed_query,
                         credential=self.__gcs_credential_json,
                         project=self.__gcs_project,
                         parentProjectId=self.__parent_project)\
                .load()\
                .persist()
            self.__common_operations.display_data_frame(final_return_data,
                                                        f"{query_type}_DATAFRAME")
            count_value = final_return_data.count()
        else:
            if not passed_labels:
                passed_labels = self.get_bq_job_labels()
            query_job_config = bigquery.QueryJobConfig(labels=passed_labels)
            final_return_data = self.__bq_client.query(
                passed_query,
                project=self.__gcs_project,
                job_config=query_job_config
            ).to_dataframe()
            count_value = len(final_return_data)

        self.__common_operations. \
            log_and_print(f"In execute_query_and_get_data_frame of BQEndpoint, "
                          f"using_spark --> {self.__using_spark}, "
                          f"count_value --> {count_value}, "
                          f"Got results :: {final_return_data}")
        return final_return_data

    def close_connection(self):
        self.__common_operations. \
            log_and_print("********** In close_connection"
                          " of BQEndpoint ********** ")
        self.reset_proxy()
