import argparse
from validation_framework.common_validate.utils import global_constants


class InputArgs:

    def __init__(self):
        print("********** Inside InputParam's __init__ method ********** ")
        parser = argparse.ArgumentParser()
        parser.add_argument("--tenant", help="tenant", required=True, default="")
        parser.add_argument("--using_spark", help="using_spark",
                            required=True, default="")
        parser.add_argument("--validation_key_name", help="validation_key_name",
                            required=True, default="")
        parser.add_argument("--validation_date", help="validation_date",
                            required=False, default=global_constants.default_run_date)
        parser.add_argument("--run_date", help="run_date",
                            required=False, default=global_constants.default_run_date)
        parser.add_argument("--using_data_compy", help="using_data_compy",
                            required=False, default="")
        parser.add_argument("--check_sample_data", help="check_sample_data",
                            required=False, default="")
        parser.add_argument("--limit_sample_data", help="limit_sample_data",
                            required=False, default="")
        parser.add_argument("--check_counts", help="check_counts",
                            required=False, default="")
        parser.add_argument("--using_temp_table", help="using_temp_table",
                            required=False, default="")
        parser.add_argument("--run_summary_only", help="run_summary_only",
                            required=False, default=global_constants.run_summary_only)
        parser.add_argument("--display_credentials", help="display_credentials",
                            required=False, default=global_constants.display_credentials)
        parser.add_argument("--table_1_type", help="table_1_type",
                            required=False, default="BQ")
        parser.add_argument("--table_2_type", help="table_2_type",
                            required=False, default="BQ")
        parser.add_argument("--results_type", help="results_type",
                            required=False, default="BQ")
        parser.add_argument("--validation_sub_type", help="validation_sub_type",
                            required=False, default="")
        parser.add_argument("--user_dir", help="user_dir",
                            required=False, default="")
        parser.add_argument("--config_file", help="config_file",
                            required=False, default=global_constants.config_file_location)
        parser.add_argument("--config_file_name", help="config_file_name",
                            required=False, default=global_constants.config_file_name)
        parser.add_argument("--keys_file", help="keys_file",
                            required=False, default="")
        parser.add_argument("--use_default_keys_file", help="use_default_keys_file",
                            required=False, default="False")
        parser.add_argument("--join_columns", help="join_columns",
                            required=False, default="")
        parser.add_argument("--comparison_criteria", help="comparison_criteria",
                            required=False, default="")
        parser.add_argument("--comparison_type", help="comparison_type",
                            required=False, default=global_constants.default_comparison_type)
        parser.add_argument("--table_full_name_1", help="table_full_name_1",
                            required=False, default="")
        parser.add_argument("--table_full_name_2", help="table_full_name_2",
                            required=False, default="")
        parser.add_argument("--where_clause_to_be_used_for_counts",
                            help="where_clause_to_be_used_for_counts",
                            required=False,
                            default=global_constants.default_where_clause_to_be_used_for_counts)
        parser.add_argument("--run_for_whole_table", help="run_for_whole_table",
                            required=False, default="")
        parser.add_argument("--where_clause", help="where_clause",
                            required=False, default="")
        parser.add_argument("--where_clause_1", help="where_clause_1",
                            required=False, default="")
        parser.add_argument("--where_clause_2", help="where_clause_2",
                            required=False, default="")
        parser.add_argument("--query_1", help="query_1",
                            required=False, default="")
        parser.add_argument("--query_2", help="query_2",
                            required=False, default="")
        parser.add_argument("--select_columns", help="select_columns",
                            required=False, default="")
        parser.add_argument("--select_columns_from_table_1", help="select_columns_from_table_1",
                            required=False, default="")
        parser.add_argument("--select_columns_from_table_2", help="select_columns_from_table_2",
                            required=False, default="")
        parser.add_argument("--ignore_columns", help="ignore_columns",
                            required=False, default="")
        parser.add_argument("--ignore_columns_from_table_1", help="ignore_columns_from_table_1",
                            required=False, default="")
        parser.add_argument("--ignore_columns_from_table_2", help="ignore_columns_from_table_2",
                            required=False, default="")
        parser.add_argument("--limit_samples_count", help="limit_samples_count",
                            required=False, default="")
        parser.add_argument("--limit_samples_count_from_table_1", help="limit_samples_count_from_table_1",
                            required=False, default="")
        parser.add_argument("--limit_samples_count_from_table_2", help="limit_samples_count_from_table_2",
                            required=False, default="")
        parser.add_argument("--special_columns", help="special_columns",
                            required=False, default="")
        parser.add_argument("--special_columns_from_table_1", help="special_columns_from_table_1",
                            required=False, default="")
        parser.add_argument("--special_columns_from_table_2", help="special_columns_from_table_2",
                            required=False, default="")
        parser.add_argument("--date_time_columns", help="date_time_columns",
                            required=False, default="")
        parser.add_argument("--date_time_columns_from_table_1", help="date_time_columns_from_table_1",
                            required=False, default="")
        parser.add_argument("--date_time_columns_from_table_2", help="date_time_columns_from_table_2",
                            required=False, default="")
        parser.add_argument("--date_type_group_by_keys", help="date_type_group_by_keys",
                            required=False, default="")
        parser.add_argument("--other_group_by_keys", help="other_group_by_keys",
                            required=False, default="")
        parser.add_argument("--type_code_columns", help="type_code_columns",
                            required=False, default="")
        parser.add_argument("--amount_columns", help="amount_columns",
                            required=False, default="")
        parser.add_argument("--check_null_columns", help="check_null_columns",
                            required=False, default="")
        parser.add_argument("--sample_record_counts", help="sample_record_counts",
                            required=False, default=global_constants.default_sample_record_counts)

        self.args = parser.parse_args()
        self.tenant = self.args.tenant
        self.using_spark = self.args.using_spark
        self.run_date = self.args.run_date
        self.validation_key_name = self.args.validation_key_name
        self.validation_date = self.args.validation_date
        self.display_credentials = self.args.display_credentials
        self.table_1_type = self.args.table_1_type
        self.table_2_type = self.args.table_2_type
        self.validation_sub_type = self.args.validation_sub_type
        self.user_dir = self.args.user_dir
        self.config_file = self.args.config_file
        self.config_file_name = self.args.config_file_name
        self.keys_file = self.args.keys_file
        self.use_default_keys_file = self.args.use_default_keys_file
        if self.keys_file:
            print(f'keys_dir --> {self.keys_file}')

        print(f"self.args --> {self.args}, "
              f"self.tenant --> {self.tenant}, "
              f"self.using_spark --> {self.using_spark}, "
              f"self.run_date --> {self.run_date}, "
              f"self.validation_key_name --> {self.validation_key_name}, "
              f"self.validation_date --> {self.validation_date}, "
              f"self.display_credentials --> {self.display_credentials}, "
              f"self.table_1_type --> {self.table_1_type}, "
              f"self.table_2_type --> {self.table_2_type}, "
              f"self.validation_sub_type --> {self.validation_sub_type}, "
              f"self.config_file --> {self.config_file}, "
              f"self.config_file_name --> {self.config_file_name}, "
              f"self.keys_file --> {self.keys_file}, "
              f"self.use_default_keys_file --> {self.use_default_keys_file}")
