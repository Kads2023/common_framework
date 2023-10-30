import base64
import requests
import json
from validation_framework.common_validate.jobparams.job_param import JobParam
from validation_framework.common_validate.utils.common_utils import CommonOperations


class KeyMakerApiProxy:
    def __init__(self, passed_job_params: JobParam, passed_common_operations: CommonOperations):
        self.__job_params = passed_job_params
        self.__common_operations = passed_common_operations

    def get_keymaker_api_response(self, keymaker_url, encoded_appcontext_path, cac_ert_path=None):
        try:
            # Handle certificate and SSL verification
            if not cac_ert_path:
                cac_ert_path = False

            # Handle keymaker token
            try:
                with open(encoded_appcontext_path) as f:
                    token = f.read()
                    self.__common_operations.log_and_print("Appcontext file read successfully.", print_msg=True)
                    token_decoded = base64.b64decode(token)
            except IOError:
                error_msg = f"In get_keymaker_api_response of KeyMakerApiProxy, " \
                            f"can't find file {encoded_appcontext_path}"
                self.__common_operations.raise_value_error(error_msg)

            # strip newline from token string
            token_decoded = token_decoded.strip()
            url = f"{keymaker_url}?version_hint=last_enabled"
            headers = {"Content-Type": "application/json", "X-KM-APP-CONTEXT": token_decoded}
            response = None
            try:
                session = requests.Session()
                session.trust_env = False
                response = session.get(url, headers=headers, verify=cac_ert_path)
                self.__common_operations.log_and_print(f"Key maker "
                                                       f"response status = {response.status_code}", print_msg=True)
                if response.ok:
                    return response.json()
                else:
                    response.raise_for_status()
            except Exception as e:
                error_msg = f"In get_keymaker_api_response of KeyMakerApiProxy, " \
                            f"Unable to read KM response - "\
                            f"Status code :{response.status_code}, "\
                            f"got the exception : {e}"
                self.__common_operations.raise_value_error(error_msg)

        except Exception as ex:
            error_msg = f"In get_keymaker_api_response of KeyMakerApiProxy, " \
                        f"Failed to fetch response from KM {ex}"
            self.__common_operations.raise_value_error(error_msg)

    def get_keymaker_key(self, keymaker_response, keymaker_keyname):
        try:
            for key in keymaker_response["nonkeys"]:
                if key["nonkey"]["name"] == keymaker_keyname and key["nonkey"]["state"] == "enabled":
                    self.__common_operations.log_and_print("In get_keymaker_key of KeyMakerApiProxy "
                                                           "credential file read "
                                                           "successfully from key maker.", print_msg=True)
                    if keymaker_keyname.find('svc') != -1:
                        key_val = base64.b64decode(key["nonkey"]["encoded_key_data"])
                        try:
                            ret_val = json.loads(key_val)
                        except TypeError as te:
                            ret_val = json.dumps(key_val)
                            self.__common_operations.log_and_print("In get_keymaker_key of KeyMakerApiProxy "
                                                                   "error while reading the key_val, "
                                                                   f"te --> {te}", print_msg=True)
                        return ret_val
                    else:
                        return base64.b64decode(key["nonkey"]["encoded_key_data"]).decode("utf-8")
            error_msg = f"In get_keymaker_key of KeyMakerApiProxy, " \
                        f"keymaker_keyname --> {keymaker_keyname}"
            self.__common_operations.raise_value_error(error_msg)

        except Exception as e:
            error_msg = f"In get_keymaker_key of KeyMakerApiProxy, , " \
                        f"keymaker_keyname --> {keymaker_keyname}, " \
                        f"Key not found ...{e}"
            self.__common_operations.raise_value_error(error_msg)
