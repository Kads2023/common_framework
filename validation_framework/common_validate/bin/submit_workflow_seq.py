import argparse
import json
import sys
import time
import subprocess
from subprocess import PIPE
import re
import os
from datetime import datetime


def exec_command(command):
    """ Execute the command and return the exit status. """
    print(f'{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    print('Running system command: \n {0} \n'.format(''.join(command)))
    pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    stdo, stde = pobj.communicate()
    exit_code = pobj.returncode
    return exit_code, stdo, stde


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", help="Name of the tenant", required=True, type=str.lower)
    parser.add_argument("--cluster_name", help="Name of the cluster", required=True)
    parser.add_argument("--job_config", help="Path to the job config", required=True)
    parser.add_argument("--app_id", help="AppId for the tenant", required=True)
    parser.add_argument("--scope", help="Scope of the operation", required=False, default="prod")
    parser.add_argument("--file", help="dependencies", required=False, default="")
    return vars(parser.parse_args())


def print_error_log(_workflow_id, _job_id, _input_args):
    dataproc_logs_cmd = f"""/cloudcli/cloudprovisioncli/cloud dataproc workflow logs-v2 \
                    --workflow-id {_workflow_id} --job-id {_job_id} --minSeverity ERROR \
                    --appId {_input_args['app_id']} --scope {_input_args['scope']}"""
    exit_code, stdo, stde = exec_command(dataproc_logs_cmd)
    print(stdo.decode('utf-8'))


def submit_workflow(config, _input_args):
    """ Submits the data proc workflow command for the given job config and returns its status. """
    current_time = round(time.time() * 1000)

    _job_config = {'configs': [config]}
    file_split = _input_args['job_config'].split(".")
    workflow_job_config = f"{file_split[0]}_{current_time}.{file_split[1]}"
    with open(workflow_job_config, 'w') as _fp:
        json.dump(_job_config, _fp)

    try:
        dependency_files = _input_args['file']
        if dependency_files:
            workflow_command = f"""/cloudcli/cloudprovisioncli/cloud dataproc workflow submit \
                    --file {_input_args['file']} \
                    --cluster-name {_input_args['cluster_name']} \
                    --job-config {workflow_job_config} \
                    --appId {_input_args['app_id']} \
                    --scope {_input_args['scope']} \
                    --disable-workflow-timeout"""
        else:
            workflow_command = f"""/cloudcli/cloudprovisioncli/cloud dataproc workflow submit \
                    --cluster-name {_input_args['cluster_name']} \
                    --job-config {workflow_job_config} \
                    --appId {_input_args['app_id']} \
                    --scope {_input_args['scope']} \
                    --disable-workflow-timeout"""

        exit_code, stdo, stde = exec_command(workflow_command)
        st_output = stdo.decode("utf-8").replace("\n", '')
        regex_match = re.search(r"Workflow ID: (?P<workflow_id>[a-z0-9-]+), "
                                r"Status: (?P<status>(DONE|FAILED)), "
                                r"Timeout: Job ID: (?P<job_id>.*),", st_output)
        print(f'{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        if regex_match:
            _status = regex_match.group('status')
            _workflow_id = regex_match.group('workflow_id')
            _job_id = regex_match.group('job_id')
            print(f"Command output: \n status --> {_status}, "
                  f"workflow_id --> {_workflow_id}, "
                  f"job_id --> {_job_id}")
            if _status == "FAILED":
                print("Getting logs of the failed job...")
                print_error_log(_workflow_id, _job_id, _input_args)
            return _status, _workflow_id, _job_id
        else:
            print(f"Problem in executing command :: "
                  f"{exit_code} :: {stdo} :: "
                  f"{stde}".replace("'", "").replace('"', '').replace('\n', ''))
            return "FAILED", "None", "None"
    finally:
        os.remove(workflow_job_config)


def check_cluster_exist(_input_args):
    get_cluster_command = f"""/cloudcli/cloudprovisioncli/cloud dataproc 
        cluster get {_input_args['cluster_name']} 
        --appId {_input_args['app_id']} 
        --scope {_input_args['scope']}"""
    exit_code, stdo, stde = exec_command(get_cluster_command)
    if stde and "ErrorNotFound" in stde.decode("utf-8"):
        print(f"Cluster - {_input_args['cluster_name']} - not available.")
        return False
    return True


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_args = parse_args()

    print("**** Checking cluster exists :: Start ****** \n")
    if check_cluster_exist:
        print(f"Cluster - {input_args['cluster_name']} - is available. So submitting the workflows.")
        print("**** Submitting workflow jobs sequentially :: Start ****** \n")
        with open(input_args['job_config']) as fp:
            job_config = json.load(fp)

        is_failed = False
        result = []
        for sub_config in job_config["configs"]:
            validation_key_name_arg = [arg for arg in sub_config["args"]
                                       if arg.startswith('--validation_key_name')]
            if validation_key_name_arg:
                input_validation_key_name_id = validation_key_name_arg[0].replace("--validation_key_name=", "")
                now_job_start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                print(f"======== Executing workflow start for {input_validation_key_name_id} =========")
                status, workflow_id, job_id = submit_workflow(sub_config, input_args)
                now_job_end_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                result.append((input_validation_key_name_id, status,
                               workflow_id, job_id,
                               now_job_start_time, now_job_end_time))
                is_failed = is_failed or status == "FAILED"

        print("****** Consolidated status *********")
        print("{:<40} {:<7} {:<40} {:<30} {:<15} {:<15}".format(
            'ValidationKeyName', 'Status', 'WorkflowId', 'JobId', 'JobStartTime', 'JobEndTime'))
        for input_validation_key_name_id, status, workflow_id, job_id, job_start_time, job_end_time in result:
            print("{:<40} {:<7} {:<40} {:<30} {:<15} {:<15}".format(
                input_validation_key_name_id, status, workflow_id, job_id, job_start_time, job_end_time))
        end_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"START_TIME --> {start_time}, END_TIME --> {end_time}")
        if is_failed:
            sys.exit(1)
    else:
        print(f"Cluster - {input_args['cluster_name']} - not available. So exiting the process.")
        raise Exception("Cluster not available. Please restart from SET_JOB_ARGS")
