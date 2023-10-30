import json
import csv


def read_json_file(file_path):
    with open(file_path, 'r') as fh:
        json_data = json.load(fh, strict=False)
    fh.close()
    return json_data


def form_where_clause(passed_where_clause, passed_key_name):
    formed_where_clause = passed_where_clause
    if passed_where_clause != "":
        used_where_clause = passed_where_clause.strip().lower()
        where_clause_values = passed_where_clause.split("=")
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


def main():
    base_location = "/Users/kasss/PycharmProjects/" \
                     "validation_framework/example_validate/conf/"
    keys_json_path = f"{base_location}keys_new.json"
    print(f"keys_json_path --> {keys_json_path}")
    keys_json_values = read_json_file(keys_json_path)
    table_keys = list(keys_json_values.keys())
    list_of_table_dict = []
    for each_key in table_keys:
        table_dict= {}
        print(f"each_key --> {each_key}")
        now_key_value = keys_json_values.get(each_key, {})

        sample_records = now_key_value.get("SAMPLE_RECORD_COUNT", "100")
        table_dict["SAMPLE_RECORD_COUNT"] = sample_records

        where_clause_to_be_used_for_counts = \
            now_key_value.get("WHERE_CLAUSE_TO_BE_USED_FOR_COUNTS", "True")
        table_dict["WHERE_CLAUSE_TO_BE_USED_FOR_COUNTS"] = where_clause_to_be_used_for_counts

        table_dict["COMPARISON_TYPE"] = "SIMBA_MIGRATION"
        table_dict["COMPARISON_CRITERIA"] = "validation_date"

        date_time_columns = now_key_value.get("DATE_TIME_COLUMNS", [])
        table_dict["DATE_TIME_COLUMNS"] = ';'.join(date_time_columns)

        join_keys = now_key_value.get("JOIN_KEYS", [])
        table_dict["JOIN_KEYS"] = ';'.join(join_keys)

        date_type_group_by_keys = now_key_value.get("DATE_TYPE_GROUP_BY_KEYS", [])
        table_dict["DATE_TYPE_GROUP_BY_KEYS"] = ';'.join(date_type_group_by_keys)

        other_group_by_keys = now_key_value.get("OTHER_GROUP_BY_KEYS", [])
        table_dict["OTHER_GROUP_BY_KEYS"] = ';'.join(other_group_by_keys)

        amount_columns = now_key_value.get("AMOUNT_COLUMNS", [])
        table_dict["AMOUNT_COLUMNS"] = ';'.join(amount_columns)

        type_code_columns = now_key_value.get("TYPE_CODE_COLUMNS", [])
        table_dict["TYPE_CODE_COLUMNS"] = ';'.join(type_code_columns)

        check_null_column = now_key_value.get("CHECK_NULL_COLUMN", [])
        table_dict["CHECK_NULL_COLUMN"] = ';'.join(check_null_column)

        common_where_clause = str(now_key_value.get("WHERE_CLAUSE", "")).strip()
        final_common_where_clause = form_where_clause(common_where_clause, each_key)
        print(f"each_key --> {each_key}, "
              f"final_common_where_clause --> {final_common_where_clause}")

        table_1_details = now_key_value.get("TABLE_1_DETAILS", {})
        table_2_details = now_key_value.get("TABLE_2_DETAILS", {})

        table_1_full_name = table_1_details.get("FULL_TABLE_NAME", "")
        table_1_where_clause = str(table_1_details.get("WHERE_CLAUSE", "")).strip()
        final_table_1_where_clause = form_where_clause(table_1_where_clause, each_key)
        print(f"each_key --> {each_key}, "
              f"final_table_1_where_clause --> {final_table_1_where_clause}")

        table_2_full_name = table_2_details.get("FULL_TABLE_NAME", "")
        table_2_where_clause = str(table_2_details.get("WHERE_CLAUSE", "")).strip()
        final_table_2_where_clause = form_where_clause(table_2_where_clause, each_key)
        print(f"each_key --> {each_key}, "
              f"final_table_2_where_clause --> {final_table_2_where_clause}")

        table_dict["TABLE_FULL_NAME_1"] = table_1_full_name
        table_dict["TABLE_FULL_NAME_2"] = table_2_full_name

        if final_table_1_where_clause == final_table_2_where_clause:
            if final_common_where_clause != "":
                table_dict["WHERE_CLAUSE"] = final_common_where_clause
                table_dict["WHERE_CLAUSE_1"] = ""
                table_dict["WHERE_CLAUSE_2"] = ""
            else:
                if final_table_1_where_clause != "":
                    table_dict["WHERE_CLAUSE"] = final_table_1_where_clause
                    table_dict["WHERE_CLAUSE_1"] = ""
                    table_dict["WHERE_CLAUSE_2"] = ""
                else:
                    print("COMMON WHERE CLAUSE IS EMPTY AND "
                          "WHERE CLAUSE 1 & 2 ARE ALSO EMPTY")
        else:
            table_dict["WHERE_CLAUSE"] = ""
            table_dict["WHERE_CLAUSE_1"] = final_table_1_where_clause
            table_dict["WHERE_CLAUSE_2"] = final_table_2_where_clause

        table_name_1 = table_1_full_name.split('.')[-1]
        table_name_2 = table_2_full_name.split('.')[-1]
        if table_name_1 == table_name_2:
            table_name = table_name_1
        else:
            table_name = ""
            print(f"ISSUE WITH TABLE NAMES, "
                  f"table_name --> {table_name}, "
                  f"table_name_1 --> {table_name_1}, "
                  f"table_name_2 --> {table_name_2}")
            exit(1)

        table_dict["KEY_NAME"] = table_name
        list_of_table_dict.append(table_dict)

    validation_details_file = f"{base_location}validation_details.csv"
    print(f"validation_details_file --> {validation_details_file}")
    with open(validation_details_file, "w") as input_csv_file:
        csv_file = csv.writer(input_csv_file)
        csv_file.writerow(["KEY_NAME", "SAMPLE_RECORD_COUNT",
                           "COMPARISON_TYPE", "COMPARISON_CRITERIA",
                           "DATE_TIME_COLUMNS", "JOIN_KEYS",
                           "DATE_TYPE_GROUP_BY_KEYS", "OTHER_GROUP_BY_KEYS",
                           "AMOUNT_COLUMNS", "TYPE_CODE_COLUMNS", "CHECK_NULL_COLUMN",
                           "WHERE_CLAUSE_TO_BE_USED_FOR_COUNTS",
                           "WHERE_CLAUSE", "TABLE_FULL_NAME_1",
                           "TABLE_FULL_NAME_2", "WHERE_CLAUSE_1", "WHERE_CLAUSE_2"])
        for each_table_dict in list_of_table_dict:
            csv_file.writerow([each_table_dict.get("KEY_NAME", ""),
                               each_table_dict.get("SAMPLE_RECORD_COUNT", ""),
                               each_table_dict.get("COMPARISON_TYPE", ""),
                               each_table_dict.get("COMPARISON_CRITERIA", ""),
                               each_table_dict.get("DATE_TIME_COLUMNS", ""),
                               each_table_dict.get("JOIN_KEYS", ""),
                               each_table_dict.get("DATE_TYPE_GROUP_BY_KEYS", ""),
                               each_table_dict.get("OTHER_GROUP_BY_KEYS", ""),
                               each_table_dict.get("AMOUNT_COLUMNS", ""),
                               each_table_dict.get("TYPE_CODE_COLUMNS", ""),
                               each_table_dict.get("CHECK_NULL_COLUMN", ""),
                               each_table_dict.get("WHERE_CLAUSE_TO_BE_USED_FOR_COUNTS", ""),
                               each_table_dict.get("WHERE_CLAUSE", ""),
                               each_table_dict.get("TABLE_FULL_NAME_1", ""),
                               each_table_dict.get("TABLE_FULL_NAME_2", ""),
                               each_table_dict.get("WHERE_CLAUSE_1", ""),
                               each_table_dict.get("WHERE_CLAUSE_2", "")])
    input_csv_file.close()


if __name__ == '__main__':
    main()
