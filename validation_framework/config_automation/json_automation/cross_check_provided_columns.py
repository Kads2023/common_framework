import json
import csv
from google.cloud import bigquery

import os
import base64
import requests

date_time_types = ["DATE", "TIME", "DATETIME", "TIMESTAMP"]


def read_json_file(file_path):
    with open(file_path, 'r') as fh:
        json_data = json.load(fh, strict=False)
    fh.close()
    return json_data


def append_issue_string(base_str, append_str, delimiter="", prefix=""):
    if base_str:
        if append_str:
            if str(append_str).startswith(";"):
                return_str = f"{base_str}{append_str}"
            else:
                if not delimiter:
                    return_str = f"{base_str}| {append_str}"
                else:
                    return_str = f"{base_str}| {delimiter}{append_str}"
        else:
            return_str = base_str
    else:
        if append_str:
            if not prefix:
                return_str = append_str
            else:
                return_str = f"{prefix}{append_str}"
        else:
            return_str = ""
    return return_str


def validate_column_in_schema(passed_key_name,
                              passed_column_name,
                              passed_column_type,
                              passed_table_1_columns,
                              passed_table_2_columns
                              ):
    print(f"Inside validate_column_in_schema, "
          f"passed_key_name --> {passed_key_name}, "
          f"passed_column_name --> {passed_column_name}")
    ret_issue_str = ""
    ret_bool = False
    if passed_column_name:
        if passed_column_name not in passed_table_1_columns:
            if passed_column_name.lower() in passed_table_1_columns:
                print(f"{passed_column_name} / {passed_column_type} "
                      f"AVAILABLE IN LOWER CASE IN TABLE_1 of {passed_key_name}")
                ret_issue_str = append_issue_string(
                    ret_issue_str,
                    f"{passed_column_name} / {passed_column_type} "
                    f"AVAILABLE IN LOWER CASE IN TABLE_1 of {passed_key_name}"
                )
                ret_bool = True
            elif passed_column_name.upper() in passed_table_1_columns:
                print(f"{passed_column_name} / {passed_column_type} "
                      f"AVAILABLE IN UPPER CASE IN TABLE_1 of {passed_key_name}")
                ret_issue_str = append_issue_string(
                    ret_issue_str,
                    f"{passed_column_name} / {passed_column_type} "
                    f"AVAILABLE IN UPPER CASE IN TABLE_1 of {passed_key_name}"
                )
                ret_bool = True
            else:
                print(f"{passed_column_name} / {passed_column_type} "
                      f"NOT AVAILABLE IN TABLE_1 of {passed_key_name}")
                ret_issue_str = append_issue_string(
                    ret_issue_str,
                    f"{passed_column_name} / {passed_column_type} "
                    f"NOT AVAILABLE IN TABLE_1 of {passed_key_name}"
                )
                ret_bool = True
        if passed_column_name not in passed_table_2_columns:
            if passed_column_name.lower() in passed_table_1_columns:
                print(f"{passed_column_name} / {passed_column_type} "
                      f"AVAILABLE IN LOWER CASE IN TABLE_2 of {passed_key_name}")
                ret_issue_str = append_issue_string(
                    ret_issue_str,
                    f"{passed_column_name} / {passed_column_type} "
                    f"AVAILABLE IN LOWER CASE IN TABLE_2 of {passed_key_name}"
                )
                ret_bool = True
            elif passed_column_name.upper() in passed_table_1_columns:
                print(f"{passed_column_name} / {passed_column_type} "
                      f"AVAILABLE IN UPPER CASE IN TABLE_2 of {passed_key_name}")
                ret_issue_str = append_issue_string(
                    ret_issue_str,
                    f"{passed_column_name} / {passed_column_type} "
                    f"AVAILABLE IN UPPER CASE IN TABLE_2 of {passed_key_name}"
                )
                ret_bool = True
            else:
                print(f"{passed_column_name} / {passed_column_type} "
                      f"NOT AVAILABLE IN TABLE_2 of {passed_key_name}")
                ret_issue_str = append_issue_string(
                    ret_issue_str,
                    f"{passed_column_name} / {passed_column_type} "
                    f"NOT AVAILABLE IN TABLE_2 of {passed_key_name}"
                )
                ret_bool = True
    else:
        print(f"{passed_column_name} / {passed_column_type} IS EMPTY")
        ret_issue_str = f"{passed_column_name} / {passed_column_type} IS EMPTY"
        ret_bool = True
    return ret_issue_str, ret_bool


def get_where_column(passed_where_clause, passed_key_name):
    ret_column_name = passed_where_clause
    if passed_where_clause != "":
        used_where_clause = passed_where_clause.strip().lower()
        where_clause_values = passed_where_clause.split("=")
        column_name = where_clause_values[0].strip().lower()
        print(f"inside get_where_column, "
              f"passed_key_name --> {passed_key_name}, "
              f"passed_where_clause --> {passed_where_clause}, "
              f"used_where_clause --> {used_where_clause}, "
              f"where_clause_values --> {where_clause_values}, "
              f"column_name --> {column_name}")
        if column_name.startswith("cast("):
            if column_name.endswith(" as date)"):
                print(f"inside get_where_column, "
                      f"in column_name starts with cast( and "
                      f"ends with as date), "
                      f"passed_key_name --> {passed_key_name}, "
                      f"column_name --> {column_name}")
                ret_column_name = column_name.replace(
                    "cast(", "").replace(
                    "as date)", "").strip()
            else:
                print(f"inside get_where_column, "
                      f"in column_name starts with cast( and "
                      f"does not ends with as date), "
                      f"passed_key_name --> {passed_key_name}, "
                      f"column_name --> {column_name}")
                ret_column_name = column_name.split("as")[0].replace(
                    "cast(", "").strip()
        elif column_name.startswith("date("):
            print(f"inside get_where_column, "
                  f"in column_name starts with date(, "
                  f"passed_key_name --> {passed_key_name}, "
                  f"column_name --> {column_name}")
            ret_column_name = column_name.replace(
                "date(", "").replace(
                ")", "").strip()
        else:
            print(f"inside get_where_column, "
                  f"in column_name does not start with "
                  f"date( or cast(, "
                  f"passed_key_name --> {passed_key_name}, "
                  f"column_name --> {column_name}")
            ret_column_name = column_name.strip()
    print(f"inside get_where_column, "
          f"passed_key_name --> {passed_key_name}, "
          f"ret_column_name --> {ret_column_name}")
    return ret_column_name


def get_bq_table_schema(bq_client,
                        project,
                        bq_dataset,
                        table_name,
                        compute_project,
                        bq_labels=None):
    bq_schema = {}
    query = f"SELECT * FROM {project}.{bq_dataset}.INFORMATION_SCHEMA.COLUMNS " \
            f"WHERE TABLE_NAME='{table_name}'"
    print(f"Inside get_bq_table_schema, "
          f"Query to get bq table schema : {query}")

    # Setting query job labels
    if bq_labels:
        job_config = bigquery.QueryJobConfig(labels=bq_labels)
    else:
        job_config = bigquery.QueryJobConfig()

    query_job = bq_client.query(query, project=compute_project, job_config=job_config)
    rows = query_job.result()
    for row in rows:
        bq_schema[row['column_name']] = row['data_type']
    return bq_schema


def get_keymaker_api_response(keymaker_url, encoded_appcontext_path, cacert_path=None):
    try:
        # Handle certificate and SSL verification
        if not cacert_path:
            cacert_path = False
        # Handle keymaker token
        try:
            with open(encoded_appcontext_path) as f:
                token = f.read()
                print("Appcontext file read successfully.")
                token_decoded = base64.b64decode(token)
        except IOError:
            raise ValueError("can't find file {}".format(encoded_appcontext_path))
        # strip newline from token string
        token_decoded = token_decoded.strip()
        url = "{}?version_hint=last_enabled".format(keymaker_url)
        headers = {"Content-Type": "application/json", "X-KM-APP-CONTEXT": token_decoded}
        try:
            session = requests.Session()
            session.trust_env = False
            response = session.get(url, headers=headers, verify=cacert_path)
            print("Key maker response status = {}".format(response.status_code))
            if response.ok:
                return response.json()
            else:
                response.raise_for_status()
        except Exception as e:
            print("Unable to read KM response - Status code :{}".format(response.status_code))
            raise e
    except Exception as ex:
        print("Failed to fetch response from KM {}".format(ex))
        raise RuntimeError("Failed to fetch response from KM " + ex)


def get_keymaker_key(keymaker_response, keymaker_keyname):
    try:
        for key in keymaker_response["nonkeys"]:
            if key["nonkey"]["name"] == keymaker_keyname and key["nonkey"]["state"] == "enabled":
                print("KeyMakerApiProxy credential file read successfully from key maker.")
                if keymaker_keyname.find('svc') != -1:
                    return json.loads(base64.b64decode(key["nonkey"]["encoded_key_data"]))
                else:
                    return base64.b64decode(key["nonkey"]["encoded_key_data"]).decode("utf-8")
        raise ValueError(keymaker_keyname)
    except Exception as e:
        print("Key not found ...{}".format(e))
        raise RuntimeError('Key not found :: ' + e)


def main():
    base_location = "/" \
                    "scripts/validation_adhoc_scripts/"

    gcs_project = "<>"

    km_credentials = "<>"

    keymaker_response = get_keymaker_api_response(
        'https://',
        '/adjacencies_connector.identity')

    gcs_credential_json = get_keymaker_key(keymaker_response, km_credentials)

    bq_client = bigquery.Client.from_service_account_info(gcs_credential_json)

    keys_json_path = f"{base_location}/keys.json"
    dt_compatibility_mapping = f"{base_location}/bq_compatibile_data_types.json"
    dt_compatibility_values = read_json_file(dt_compatibility_mapping)
    print(f"dt_compatibility_values --> {dt_compatibility_values}")
    dt_compatibility_values_keys = list(dt_compatibility_values.keys())
    keys_json_values = read_json_file(keys_json_path)
    table_keys = list(keys_json_values.keys())
    list_of_table_dict = []
    for each_key in table_keys:
        table_dict = {}
        table_issue = ""
        table_has_issues = False
        table_where_clause_issues = False
        print(f"each_key --> {each_key}")
        now_key_value = keys_json_values.get(each_key, {})
        table_dict["KEY_NAME"] = each_key

        seq_no = now_key_value.get("SEQ_NO", "")
        group = now_key_value.get("GROUP", "")
        table_dict["SEQ_NO"] = seq_no
        table_dict["GROUP"] = group

        table_1_details = now_key_value.get("TABLE_1_DETAILS", {})
        table_2_details = now_key_value.get("TABLE_2_DETAILS", {})

        table_1_full_name = table_1_details.get("FULL_TABLE_NAME", "")
        table_2_full_name = table_2_details.get("FULL_TABLE_NAME", "")
        table_dict["TABLE_FULL_NAME_1"] = table_1_full_name
        table_dict["TABLE_FULL_NAME_2"] = table_2_full_name

        table_1_bq_details = str(table_1_full_name).strip().split(".")
        table_2_bq_details = str(table_2_full_name).strip().split(".")

        table_1_schema = get_bq_table_schema(
            bq_client,
            table_1_bq_details[0],
            table_1_bq_details[1],
            table_1_bq_details[2],
            gcs_project
        )
        table_1_columns = list(table_1_schema.keys())

        table_2_schema = get_bq_table_schema(
            bq_client,
            table_2_bq_details[0],
            table_2_bq_details[1],
            table_2_bq_details[2],
            gcs_project
        )
        table_2_columns = list(table_2_schema.keys())

        all_columns = set(
            table_1_columns + table_2_columns
        )

        table_dict["TABLE_SCHEMA_1"] = table_1_schema
        table_dict["TABLE_SCHEMA_2"] = table_2_schema

        table_1_dt_columns = []
        table_2_dt_columns = []
        table_1_alone_columns = []
        table_2_alone_columns = []
        column_schema_issues = ""
        compatible_but_better_to_change = ""
        for each_col in all_columns:
            schema_issue = False
            avlbl_in_table_1 = False
            avlbl_in_table_2 = False
            if each_col in table_1_columns:
                avlbl_in_table_1 = True
            if each_col in table_2_columns:
                avlbl_in_table_2 = True
            each_col_dt_1 = str(table_1_schema.get(each_col, "")).strip().upper()
            each_col_dt_2 = str(table_2_schema.get(each_col, "")).strip().upper()
            if avlbl_in_table_1 and avlbl_in_table_2:
                if each_col_dt_1 != each_col_dt_2:
                    if each_col_dt_1 in dt_compatibility_values_keys:
                        now_comp_values = str(dt_compatibility_values[each_col_dt_1]).strip().split(";")
                        if each_col_dt_2 in now_comp_values:
                            compatible_but_better_to_change = append_issue_string(
                                compatible_but_better_to_change,
                                f"{each_col} --> TBL_1_DT--{each_col_dt_1} ; TBL_2_DT--{each_col_dt_2} "
                            )
                        else:
                            schema_issue = True
                    if schema_issue:
                        column_schema_issues = append_issue_string(
                            column_schema_issues,
                            f"{each_col} --> TBL_1_DT--{each_col_dt_1} ; TBL_2_DT--{each_col_dt_2} "
                        )
            elif avlbl_in_table_1 and not avlbl_in_table_2:
                table_1_alone_columns.append(f"{each_col} --> {each_col_dt_1}")
                if each_col_dt_1 in date_time_types:
                    table_1_dt_columns.append(each_col)
            elif avlbl_in_table_2 and not avlbl_in_table_1:
                table_2_alone_columns.append(f"{each_col} --> {each_col_dt_2}")
                if each_col_dt_2 in date_time_types:
                    table_2_dt_columns.append(each_col)

        table_1_dt_columns.sort()
        table_dict["TABLE_1_DATE_TIME_COLUMNS"] = table_1_dt_columns
        table_dict["COLUMNS_IN_TABLE_1_ALONE"] = table_1_alone_columns

        table_2_dt_columns.sort()
        table_dict["TABLE_2_DATE_TIME_COLUMNS"] = table_2_dt_columns
        table_dict["COLUMNS_IN_TABLE_2_ALONE"] = table_2_alone_columns

        table_dict["COLUMN_SCHEMA_ISSUES"] = column_schema_issues
        table_dict["COLUMN_SCHEMA_BETTER_TO_CHANGE"] = compatible_but_better_to_change
        table_issue = append_issue_string(
            table_issue,
            column_schema_issues
        )
        if column_schema_issues:
            table_has_issues = True
        if table_1_dt_columns == table_2_dt_columns:
            common_dt_columns = table_1_dt_columns
        else:
            common_dt_columns = set(
                table_1_dt_columns + table_2_dt_columns
            )
        table_dict["COMMON_DATE_TIME_COLUMNS"] = common_dt_columns

        table_1_where_clause = str(table_1_details.get("WHERE_CLAUSE", "")).strip()
        table_1_where_column = get_where_column(table_1_where_clause, each_key)
        print(f"each_key --> {each_key}, "
              f"table_1_where_column --> {table_1_where_column}")

        table_2_where_clause = str(table_2_details.get("WHERE_CLAUSE", "")).strip()
        table_2_where_column = get_where_column(table_2_where_clause, each_key)
        print(f"each_key --> {each_key}, "
              f"table_2_where_column --> {table_2_where_column}")

        common_where_clause = str(now_key_value.get("WHERE_CLAUSE", "")).strip()
        common_where_column = get_where_column(common_where_clause, each_key)
        print(f"each_key --> {each_key}, "
              f"common_where_column --> {common_where_column}")

        if table_1_where_column == table_2_where_column:
            if common_where_column != "":
                common_where_column_check, check_issue = \
                    validate_column_in_schema(
                        each_key,
                        common_where_column,
                        "common_where_column",
                        table_1_columns,
                        table_2_columns
                    )
                table_dict["WHERE_CLAUSE"] = common_where_column_check
                table_issue = append_issue_string(
                    table_issue,
                    common_where_column_check
                )
                if check_issue:
                    table_has_issues = check_issue
            else:
                if table_1_where_column != "":
                    common_where_column_check, check_issue = \
                        validate_column_in_schema(
                            each_key,
                            table_1_where_column,
                            "table_1_where_column",
                            table_1_columns,
                            table_2_columns
                        )
                    table_dict["WHERE_CLAUSE"] = common_where_column_check
                    table_issue = append_issue_string(
                        table_issue,
                        common_where_column_check
                    )
                    if check_issue:
                        table_has_issues = check_issue
                else:
                    common_where_column_check = "NO WHERE CLAUSE AVAILABLE"
                    table_dict["WHERE_CLAUSE"] = common_where_column_check
                    table_issue = append_issue_string(
                        table_issue,
                        common_where_column_check
                    )
                    table_where_clause_issues = True
        else:
            table_1_where_column_check, check_issue = \
                validate_column_in_schema(
                    each_key,
                    table_1_where_column,
                    "table_1_where_column",
                    table_1_columns,
                    table_2_columns
                )
            table_dict["WHERE_CLAUSE_1"] = table_1_where_column_check
            table_issue = append_issue_string(
                table_issue,
                table_1_where_column_check
            )
            if check_issue:
                table_has_issues = check_issue
            table_2_where_column_check, check_issue = \
                validate_column_in_schema(
                    each_key,
                    table_2_where_column,
                    "table_2_where_column",
                    table_1_columns,
                    table_2_columns
                )
            table_dict["WHERE_CLAUSE_2"] = table_2_where_column_check
            table_issue = append_issue_string(
                table_issue,
                table_2_where_column_check
            )
            if check_issue:
                table_has_issues = check_issue

        date_time_columns = now_key_value.get("DATE_TIME_COLUMNS", [])
        date_time_columns_issue = ""
        for each_column in date_time_columns:
            each_column_issue, check_issue = validate_column_in_schema(
                each_key,
                each_column,
                "DATE_TIME_COLUMNS",
                table_1_columns,
                table_2_columns
            )
            date_time_columns_issue = append_issue_string(
                date_time_columns_issue,
                each_column_issue
            )
            if check_issue:
                table_has_issues = check_issue
        table_issue = append_issue_string(
            table_issue,
            date_time_columns_issue
        )
        table_dict["DATE_TIME_COLUMNS"] = date_time_columns_issue

        join_keys = now_key_value.get("JOIN_KEYS", [])
        join_keys_issue = ""
        for each_column in join_keys:
            each_column_issue, check_issue = validate_column_in_schema(
                each_key,
                each_column,
                "JOIN_KEYS",
                table_1_columns,
                table_2_columns
            )
            join_keys_issue = append_issue_string(
                join_keys_issue,
                each_column_issue
            )
            if check_issue:
                table_has_issues = check_issue
        table_issue = append_issue_string(
            table_issue,
            join_keys_issue
        )
        table_dict["JOIN_KEYS"] = join_keys_issue

        date_type_group_by_keys = now_key_value.get("DATE_TYPE_GROUP_BY_KEYS", [])
        date_type_group_by_keys_column_issue = ""
        for each_column in date_type_group_by_keys:
            each_column_issue, check_issue = validate_column_in_schema(
                each_key,
                each_column,
                "DATE_TYPE_GROUP_BY_KEYS",
                table_1_columns,
                table_2_columns
            )
            date_type_group_by_keys_column_issue = append_issue_string(
                date_type_group_by_keys_column_issue,
                each_column_issue
            )
            if check_issue:
                table_has_issues = check_issue
        table_issue = append_issue_string(
            table_issue,
            date_type_group_by_keys_column_issue
        )
        table_dict["DATE_TYPE_GROUP_BY_KEYS"] = date_type_group_by_keys_column_issue

        other_group_by_keys = now_key_value.get("OTHER_GROUP_BY_KEYS", [])
        other_group_by_keys_column_issue = ""
        for each_column in other_group_by_keys:
            each_column_issue, check_issue = validate_column_in_schema(
                each_key,
                each_column,
                "OTHER_GROUP_BY_KEYS",
                table_1_columns,
                table_2_columns
            )
            other_group_by_keys_column_issue = append_issue_string(
                other_group_by_keys_column_issue,
                each_column_issue
            )
            if check_issue:
                table_has_issues = check_issue
        table_issue = append_issue_string(
            table_issue,
            other_group_by_keys_column_issue
        )
        table_dict["OTHER_GROUP_BY_KEYS"] = other_group_by_keys_column_issue

        amount_columns = now_key_value.get("AMOUNT_COLUMNS", [])
        amount_columns_issue = ""
        for each_column in amount_columns:
            each_column_issue, check_issue = validate_column_in_schema(
                each_key,
                each_column,
                "AMOUNT_COLUMNS",
                table_1_columns,
                table_2_columns
            )
            amount_columns_issue = append_issue_string(
                amount_columns_issue,
                each_column_issue
            )
            if check_issue:
                table_has_issues = check_issue
        table_issue = append_issue_string(
            table_issue,
            amount_columns_issue
        )
        table_dict["AMOUNT_COLUMNS"] = amount_columns_issue

        type_code_columns = now_key_value.get("TYPE_CODE_COLUMNS", [])
        type_code_columns_issue = ""
        for each_column in type_code_columns:
            each_column_issue, check_issue = validate_column_in_schema(
                each_key,
                each_column,
                "TYPE_CODE_COLUMNS",
                table_1_columns,
                table_2_columns
            )
            type_code_columns_issue = append_issue_string(
                type_code_columns_issue,
                each_column_issue
            )
            if check_issue:
                table_has_issues = check_issue
        table_issue = append_issue_string(
            table_issue,
            type_code_columns_issue
        )
        table_dict["TYPE_CODE_COLUMNS"] = type_code_columns_issue

        check_null_columns = now_key_value.get("CHECK_NULL_COLUMNS", [])
        check_null_columns_issue = ""
        for each_column in check_null_columns:
            each_column_issue, check_issue = validate_column_in_schema(
                each_key,
                each_column,
                "CHECK_NULL_COLUMNS",
                table_1_columns,
                table_2_columns
            )
            check_null_columns_issue = append_issue_string(
                check_null_columns_issue,
                each_column_issue
            )
            if check_issue:
                table_has_issues = check_issue
        table_issue = append_issue_string(
            table_issue,
            check_null_columns_issue
        )
        table_dict["CHECK_NULL_COLUMNS"] = check_null_columns_issue

        table_dict["ISSUES"] = table_issue
        table_dict["TABLE_HAS_ISSUES"] = table_has_issues
        table_dict["TABLE_WHERE_CLAUSE_ISSUES"] = table_where_clause_issues
        list_of_table_dict.append(table_dict)

    csv_file_name = f"{base_location}/keys_issues.csv"
    print(f"csv_file_name --> {csv_file_name}, "
          f"len(list_of_table_dict) --> {len(list_of_table_dict)}, "
          f"list_of_table_dict --> {list_of_table_dict}")

    with open(csv_file_name, "w") as input_csv_file:
        csv_file = csv.writer(input_csv_file)
        csv_file.writerow(["SEQ_NO", "GROUP",
                           "KEY_NAME", "TABLE_HAS_ISSUES",
                           "TABLE_WHERE_CLAUSE_ISSUES", "ISSUES",
                           "COLUMN_SCHEMA_ISSUES", "COLUMN_SCHEMA_BETTER_TO_CHANGE",
                           "COLUMNS_IN_TABLE_1_ALONE",
                           "COLUMNS_IN_TABLE_2_ALONE",
                           "DATE_TIME_COLUMNS", "JOIN_KEYS",
                           "DATE_TYPE_GROUP_BY_KEYS", "OTHER_GROUP_BY_KEYS",
                           "AMOUNT_COLUMNS", "TYPE_CODE_COLUMNS",
                           "CHECK_NULL_COLUMNS", "WHERE_CLAUSE",
                           "TABLE_FULL_NAME_1", "TABLE_FULL_NAME_2",
                           "WHERE_CLAUSE_1", "WHERE_CLAUSE_2",
                           "TABLE_SCHEMA_1", "TABLE_SCHEMA_2",
                           "COMMON_DATE_TIME_COLUMNS",
                           "TABLE_1_DATE_TIME_COLUMNS",
                           "TABLE_2_DATE_TIME_COLUMNS"])
        for each_table_dict in list_of_table_dict:
            csv_file.writerow([each_table_dict.get("SEQ_NO", ""),
                               each_table_dict.get("GROUP", ""),
                               each_table_dict.get("KEY_NAME", ""),
                               each_table_dict.get("TABLE_HAS_ISSUES", ""),
                               each_table_dict.get("TABLE_WHERE_CLAUSE_ISSUES", ""),
                               each_table_dict.get("ISSUES", ""),
                               each_table_dict.get("COLUMN_SCHEMA_ISSUES", ""),
                               each_table_dict.get("COLUMN_SCHEMA_BETTER_TO_CHANGE", ""),
                               each_table_dict.get("COLUMNS_IN_TABLE_1_ALONE", ""),
                               each_table_dict.get("COLUMNS_IN_TABLE_2_ALONE", ""),
                               each_table_dict.get("DATE_TIME_COLUMNS", ""),
                               each_table_dict.get("JOIN_KEYS", ""),
                               each_table_dict.get("DATE_TYPE_GROUP_BY_KEYS", ""),
                               each_table_dict.get("OTHER_GROUP_BY_KEYS", ""),
                               each_table_dict.get("AMOUNT_COLUMNS", ""),
                               each_table_dict.get("TYPE_CODE_COLUMNS", ""),
                               each_table_dict.get("CHECK_NULL_COLUMNS", ""),
                               each_table_dict.get("WHERE_CLAUSE", ""),
                               each_table_dict.get("TABLE_FULL_NAME_1", ""),
                               each_table_dict.get("TABLE_FULL_NAME_2", ""),
                               each_table_dict.get("WHERE_CLAUSE_1", ""),
                               each_table_dict.get("WHERE_CLAUSE_2", ""),
                               each_table_dict.get("TABLE_SCHEMA_1", ""),
                               each_table_dict.get("TABLE_SCHEMA_2", ""),
                               each_table_dict.get("COMMON_DATE_TIME_COLUMNS", ""),
                               each_table_dict.get("TABLE_1_DATE_TIME_COLUMNS", ""),
                               each_table_dict.get("TABLE_2_DATE_TIME_COLUMNS", "")])
    input_csv_file.close()


if __name__ == '__main__':
    main()
