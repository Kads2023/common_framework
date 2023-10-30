import pandas as pd
from config_reader import ConfigReader


class ConfigGenerator:

    def __init__(self, config, passed_excel_path, passed_sheet_name):
        self.config = config
        self.df = pd.read_excel(passed_excel_path, sheet_name=passed_sheet_name)
        self.df.fillna('', inplace=True)

    def split_and_remove_space(self, passed_str, passed_delimiter=";", passed_key_name=""):
        formed_list = str(passed_str).strip().split(passed_delimiter)
        ret_list = []
        for each in formed_list:
            fin_val = each.strip().lower()
            now_values = fin_val.split(" ")
            if len(now_values) > 1:
                print(f"{passed_key_name}, "
                      f"fin_val --> {fin_val}, "
                      f"SEEMS LIKE SOME ISSUE WITH STRING PASSED")
            now_values = fin_val.split(",")
            if len(now_values) > 1:
                print(f"{passed_key_name}, "
                      f"fin_val --> {fin_val}, "
                      f"SEEMS LIKE SOME ISSUE WITH STRING PASSED")
            ret_list.append(fin_val)
        return ret_list

    def form_where_clause(self, passed_where_clause, passed_key_name,
                          passed_hardcoded_where_clause="False"):
        formed_where_clause = passed_where_clause
        if passed_hardcoded_where_clause != "True":
            if passed_where_clause != "":
                used_where_clause = passed_where_clause.strip().lower()
                where_clause_values = self.split_and_remove_space(
                    passed_where_clause,
                    passed_delimiter="=",
                    passed_key_name=passed_key_name
                )
                print(f"inside form_where_clause, "
                      f"passed_key_name --> {passed_key_name}, "
                      f"passed_where_clause --> {passed_where_clause}, "
                      f"used_where_clause --> {used_where_clause}, "
                      f"where_clause_values --> {where_clause_values}")
                if used_where_clause.startswith("cast("):
                    if used_where_clause.endswith("validation_date'"):
                        formed_where_clause = passed_where_clause
                    else:
                        column_value = str(where_clause_values[0]).strip().lower()
                        print(f"inside form_where_clause, "
                              f"in used_where_clause "
                              f"starts with cast( and "
                              f"does not end with validation_date', "
                              f"passed_key_name --> {passed_key_name}, "
                              f"column_value --> {column_value}")
                        if column_value.endswith(" as date)"):
                            formed_where_clause = f"{column_value} = 'validation_date'"
                elif used_where_clause.startswith("date("):
                    column_value = str(where_clause_values[0]
                                       ).strip().lower().replace("date(", "").replace(")", "")
                    print(f"inside form_where_clause, "
                          f"in used_where_clause starts with date(, "
                          f"passed_key_name --> {passed_key_name}, "
                          f"column_value --> {column_value}")
                    formed_where_clause = f"CAST({column_value} AS DATE) = 'validation_date'"
                else:
                    column_value = str(where_clause_values[0]).strip()
                    print(f"inside form_where_clause, "
                          f"in used_where_clause does not start with, "
                          f"cast( or date(, "
                          f"passed_key_name --> {passed_key_name}, "
                          f"column_value --> {column_value}")
                    formed_where_clause = f"CAST({column_value} AS DATE) = 'validation_date'"
                print(f"inside form_where_clause, "
                      f"passed_key_name --> {passed_key_name}, "
                      f"formed_where_clause --> {formed_where_clause}")
        return formed_where_clause

    def crt_task(self, group="",
                 group_name="",
                 passed_excel_type=""):
        print(f"inside crt_task, "
              f"group --> {group}, "
              f"group_name --> {group_name}")
        df_group = self.df
        print("df_group")
        print(df_group)
        df_group_new = df_group
        if int(group) == 0:
            df_new = self.df[self.df[group_name] != '']
            df_group_new = df_new[df_new["key_name"] != '']
            print("df_group_new")
            print(df_group_new)
        else:
            df_group_new = self.df[self.df["group"] == int(group)]
            print(df_group_new)
            self.config["default"]["group"] = str(group)
        tasks = []
        for index, row in df_group_new.iterrows():
            task = dict()
            if passed_excel_type == "validation_excel_path":
                task["seq_no"] = str(
                    row['seq_no']
                ).strip().replace(" ", "").replace("\n", "")
                task["group"] = str(
                    row['group']
                ).strip().replace(" ", "").replace("\n", "")
                task["create_executing_stat"] = str(
                    row['create_executing_stat']
                ).strip().replace(" ", "").replace("\n", "").capitalize()
                task["run_summary_only"] = str(
                    row['run_summary_only']
                ).strip().replace(" ", "").replace("\n", "").capitalize()
                task["sample_record_count"] = str(
                    row['sample_record_count']
                ).strip().replace(" ", "").replace("\n", "")
                task["comparison_type"] = str(
                    row['comparison_type']
                ).strip().replace(" ", "").replace("\n", "")
                task["comparison_criteria"] = str(
                    row['comparison_criteria']
                ).strip().replace(" ", "").replace("\n", "")
                where_clause_to_be_used_for_counts = str(
                    row['where_clause_to_be_used_for_counts']
                ).strip().replace(" ", "").replace("\n", "").capitalize()
                table_1_full_name = str(
                    row['table_full_name_1']
                ).strip().replace(" ", "").replace("\n", "")
                table_2_full_name = str(
                    row['table_full_name_2']
                ).strip().replace(" ", "").replace("\n", "")

                table_name_1 = self.split_and_remove_space(
                    table_1_full_name, passed_delimiter='.')[-1]
                table_name_2 = self.split_and_remove_space(
                    table_2_full_name, passed_delimiter='.')[-1]
                if table_name_1 == table_name_2:
                    table_name = table_name_1
                else:
                    table_name = str(
                        row['key_name']
                    ).strip().replace(" ", "").replace("\n", "")
                    print(f"ISSUE WITH TABLE NAMES, BOTH TABLE NAMES ARE DIFFERENT, "
                          f"table_name --> {table_name}, "
                          f"table_name_1 --> {table_name_1}, "
                          f"table_name_2 --> {table_name_2}")

                task["table_name"] = table_name
                task["table_full_name_1"] = table_1_full_name
                task["table_full_name_2"] = table_2_full_name

                task["skipped"] = str(
                    row['skipped']
                ).strip().replace(" ", "").replace("\n", "")

                date_time_columns = str(
                    row['date_time_columns']
                ).strip().replace(" ", "").replace("\n", "")
                if date_time_columns != '':
                    task["date_time_columns"] = self.split_and_remove_space(
                        date_time_columns, passed_key_name=table_name)
                else:
                    task["date_time_columns"] = ''

                join_keys = str(
                    row['join_keys']
                ).strip().replace(" ", "").replace("\n", "")
                if join_keys != '':
                    task["join_keys"] = self.split_and_remove_space(
                        join_keys, passed_key_name=table_name)
                else:
                    task["join_keys"] = ''

                date_type_group_by_keys = str(
                    row['date_type_group_by_keys']
                ).strip().replace(" ", "").replace("\n", "")
                if date_type_group_by_keys != '':
                    task["date_type_group_by_keys"] = self.split_and_remove_space(
                        date_type_group_by_keys, passed_key_name=table_name)
                else:
                    task["date_type_group_by_keys"] = ''

                other_group_by_keys = str(
                    row['other_group_by_keys']
                ).strip().replace(" ", "").replace("\n", "")
                if other_group_by_keys != '':
                    task["other_group_by_keys"] = self.split_and_remove_space(
                        other_group_by_keys, passed_key_name=table_name)
                else:
                    task["other_group_by_keys"] = ''

                amount_columns = str(
                    row['amount_columns']
                ).strip().replace(" ", "").replace("\n", "")
                if amount_columns != '':
                    task["amount_columns"] = self.split_and_remove_space(
                        amount_columns, passed_key_name=table_name)
                else:
                    task["amount_columns"] = ''

                type_code_columns = str(
                    row['type_code_columns']
                ).strip().replace(" ", "").replace("\n", "")
                if type_code_columns != '':
                    task["type_code_columns"] = self.split_and_remove_space(
                        type_code_columns, passed_key_name=table_name)
                else:
                    task["type_code_columns"] = ''

                check_null_columns = str(
                    row['check_null_columns']
                ).strip().replace(" ", "").replace("\n", "")
                if check_null_columns != '':
                    task["check_null_columns"] = self.split_and_remove_space(
                        check_null_columns, passed_key_name=table_name)
                else:
                    task["check_null_columns"] = ''

                hardcoded_where_clause = str(
                    row['hardcoded_where_clause']
                ).strip().replace(" ", "").replace("\n", "").capitalize()

                common_where_clause = str(
                    row['where_clause']
                ).strip().replace("\n", "")
                final_common_where_clause = self.form_where_clause(
                    common_where_clause, table_name,
                    passed_hardcoded_where_clause=hardcoded_where_clause
                )

                where_clause_1 = str(
                    row['where_clause_1']
                ).strip().replace("\n", "")
                final_where_clause_1 = self.form_where_clause(
                    where_clause_1, table_name,
                    passed_hardcoded_where_clause=hardcoded_where_clause
                )
                where_clause_2 = str(
                    row['where_clause_2']
                ).strip().replace("\n", "")
                final_where_clause_2 = self.form_where_clause(
                    where_clause_2, table_name,
                    passed_hardcoded_where_clause=hardcoded_where_clause
                )

                run_for_whole_table = str(
                    row['run_for_whole_table']
                ).strip().replace(" ", "").replace("\n", "").capitalize()

                if final_where_clause_2 == final_where_clause_2:
                    if final_common_where_clause != "":
                        final_where_clause_1 = \
                            final_where_clause_2 = ''
                    else:
                        if final_where_clause_1 != "":
                            final_common_where_clause = final_where_clause_1
                        else:
                            run_for_whole_table = 'True'
                            where_clause_to_be_used_for_counts = ''
                            print(f"ISSUE WITH WHERE CLAUSE, "
                                  f"table_name --> {table_name}, "
                                  f"final_where_clause_2 --> {final_where_clause_2}, "
                                  f"final_where_clause_2 --> {final_where_clause_2}")
                            # exit(1)
                else:
                    final_common_where_clause = ""
                task["where_clause_to_be_used_for_counts"] = where_clause_to_be_used_for_counts
                task["run_for_whole_table"] = run_for_whole_table
                task["where_clause_1"] = final_where_clause_1
                task["where_clause_2"] = final_where_clause_2
                task["where_clause"] = final_common_where_clause

                now_common_select_columns = str(
                    row['select_columns']
                ).strip().replace("\n", "")
                if now_common_select_columns != '':
                    task["select_columns"] = self.split_and_remove_space(
                        now_common_select_columns, passed_key_name=table_name)
                else:
                    task["select_columns"] = ''

                now_select_columns_1 = str(
                    row['select_columns_1']
                ).strip().replace("\n", "")
                if now_select_columns_1 != '':
                    task["select_columns_1"] = self.split_and_remove_space(
                        now_select_columns_1, passed_key_name=table_name)
                else:
                    task["select_columns_1"] = ''

                now_select_columns_2 = str(
                    row['select_columns_2']
                ).strip().replace("\n", "")
                if now_select_columns_2 != '':
                    task["select_columns_2"] = self.split_and_remove_space(
                        now_select_columns_2, passed_key_name=table_name)
                else:
                    task["select_columns_2"] = ''

                now_common_ignore_columns = str(
                    row['ignore_columns']
                ).strip().replace("\n", "")
                if now_common_ignore_columns != '':
                    task["ignore_columns"] = self.split_and_remove_space(
                        now_common_ignore_columns, passed_key_name=table_name)
                else:
                    task["ignore_columns"] = ''

                now_ignore_columns_1 = str(
                    row['ignore_columns_1']
                ).strip().replace("\n", "")
                if now_ignore_columns_1 != '':
                    task["ignore_columns_1"] = self.split_and_remove_space(
                        now_ignore_columns_1, passed_key_name=table_name)
                else:
                    task["ignore_columns_1"] = ''

                now_ignore_columns_2 = str(
                    row['ignore_columns_2']
                ).strip().replace("\n", "")
                if now_ignore_columns_2 != '':
                    task["ignore_columns_2"] = self.split_and_remove_space(
                        now_ignore_columns_2, passed_key_name=table_name)
                else:
                    task["ignore_columns_2"] = ''
            else:
                print(f"UNKNOWN, passed_excel_type --> {passed_excel_type}")
            tasks.append(task)
        tasks_dict = dict()
        tasks_dict['tasks'] = tasks
        # print(f"tasks_dict --> {tasks_dict}")
        return tasks_dict

