from datetime import datetime, timedelta
import json
from calendar import monthrange


def set_job_arguments(ds, **kwargs):
    local_tz = kwargs['local_tz']
    schedule_interval_mins = kwargs['schedule_interval_mins']
    print("Values coming from airflow kwargs for data_interval_start & data_interval_end")
    print(kwargs['data_interval_start'])
    print(kwargs['data_interval_end'])
    execution_datetime = local_tz.convert(kwargs['data_interval_start'])
    next_execution_datetime = local_tz.convert(kwargs['data_interval_end'])
    validation_datetime = local_tz.convert(kwargs['data_interval_start'])
    if 'required_delay' in kwargs.keys():
        required_delay_mins = int(kwargs['required_delay'])
        print("Required delay is true. Delay mins = " + str(required_delay_mins))
        execution_datetime = execution_datetime - timedelta(minutes=required_delay_mins)
        next_execution_datetime = next_execution_datetime - timedelta(minutes=required_delay_mins)
    if 'required_validation_delay' in kwargs.keys():
        required_validation_delay = int(kwargs['required_validation_delay'])
        print("Required validation delay is true. Delay days = " + str(required_validation_delay))
        validation_datetime = validation_datetime - timedelta(days=required_validation_delay)
    print("Execution batch start = " + str(execution_datetime))
    print("Execution batch end = " + str(next_execution_datetime))
    run_date = (datetime.strptime(str(execution_datetime)[:10], '%Y-%m-%d')).strftime('%Y%m%d')
    kwargs['task_instance'].xcom_push(key='run_date', value=run_date)
    print("Run date to execute = " + str(run_date))
    sequence_no = int((execution_datetime.hour * 60 + execution_datetime.minute) / schedule_interval_mins) + 1
    print("sequence number to execute = " + str(sequence_no))
    kwargs['task_instance'].xcom_push(key='sequence_no', value=sequence_no)
    min_run_id_to_process = execution_datetime.strftime('%Y%m%d235959')
    print("Minimum run id to process = " + min_run_id_to_process)
    kwargs['task_instance'].xcom_push(key='min_run_id_to_process', value=min_run_id_to_process)
    validation_date = (datetime.strptime(str(validation_datetime)[:10], '%Y-%m-%d')).strftime('%Y-%m-%d')
    kwargs['task_instance'].xcom_push(key='validation_date', value=validation_date)
    print("Validation date to execute = " + str(validation_date))


def set_job_arguments_monthly(ds, **kwargs):
    local_tz = kwargs['local_tz']
    schedule_interval_mins = kwargs['schedule_interval_mins']
    print("Values coming from airflow kwargs for data_interval_start & data_interval_end")
    print(kwargs['data_interval_start'])
    print(kwargs['data_interval_end'])
    execution_datetime = local_tz.convert(kwargs['data_interval_start'])
    next_execution_datetime = local_tz.convert(kwargs['data_interval_end'])
    if('required_delay' in kwargs.keys()):
        required_delay_mins = int(kwargs['required_delay'])
        print("Required delay is true. Delay mins = " + str(required_delay_mins))
        execution_datetime = execution_datetime - timedelta(minutes=required_delay_mins)
        next_execution_datetime = next_execution_datetime - timedelta(minutes=required_delay_mins)
    print("Execution batch start = " + str(execution_datetime))
    print("Execution batch end = " + str(next_execution_datetime))
    run_date = (datetime.strptime(str(execution_datetime)[:10], '%Y-%m-%d')).strftime('%Y%m%d')
    kwargs['task_instance'].xcom_push(key='run_date', value=run_date)
    print("Run date to execute = " + str(run_date))
    sequence_no = int((execution_datetime.hour * 60 + execution_datetime.minute) / schedule_interval_mins) + 1
    print("sequence number to execute = " + str(sequence_no))
    kwargs['task_instance'].xcom_push(key='sequence_no', value=sequence_no)
    min_run_id_to_process = (next_execution_datetime - timedelta(seconds=1)).strftime('%Y%m%d%H%M%S')
    print("Minimum run id to process = " + min_run_id_to_process)
    kwargs['task_instance'].xcom_push(key='min_run_id_to_process', value=min_run_id_to_process)


def set_job_arguments_validations(ds, **kwargs):
    local_tz = kwargs['local_tz']
    schedule_interval_mins = kwargs['schedule_interval_mins']
    group_id = kwargs['group_id']
    print("Values coming from airflow kwargs for data_interval_start & data_interval_end")
    print(kwargs['data_interval_start'])
    print(kwargs['data_interval_end'])
    execution_datetime = local_tz.convert(kwargs['data_interval_start'])
    next_execution_datetime = local_tz.convert(kwargs['data_interval_end'])
    validation_datetime = local_tz.convert(kwargs['data_interval_start'])
    if 'required_delay' in kwargs.keys():
        required_delay_mins = int(kwargs['required_delay'])
        print("Required delay is true. Delay mins = " + str(required_delay_mins))
        execution_datetime = execution_datetime - timedelta(minutes=required_delay_mins)
        next_execution_datetime = next_execution_datetime - timedelta(minutes=required_delay_mins)
    if 'required_validation_delay' in kwargs.keys():
        required_validation_delay = int(kwargs['required_validation_delay'])
        print("Required validation delay is true. Delay days = " + str(required_validation_delay))
        validation_datetime = validation_datetime - timedelta(days=required_validation_delay)
    print("Execution batch start = " + str(execution_datetime))
    print("Execution batch end = " + str(next_execution_datetime))
    run_date = (datetime.strptime(str(execution_datetime)[:10], '%Y-%m-%d')).strftime('%Y%m%d')
    kwargs['task_instance'].xcom_push(key='run_date', value=run_date)
    print("Run date to execute = " + str(run_date))
    sequence_no = int((execution_datetime.hour * 60 + execution_datetime.minute) / schedule_interval_mins) + 1
    print("sequence number to execute = " + str(sequence_no))
    kwargs['task_instance'].xcom_push(key='sequence_no', value=sequence_no)
    min_run_id_to_process = execution_datetime.strftime('%Y%m%d235959')
    print("Minimum run id to process = " + min_run_id_to_process)
    kwargs['task_instance'].xcom_push(key='min_run_id_to_process', value=min_run_id_to_process)
    validation_date = (datetime.strptime(str(validation_datetime)[:10], '%Y-%m-%d')).strftime('%Y-%m-%d')
    kwargs['task_instance'].xcom_push(key='validation_date', value=validation_date)
    print("Validation date to execute = " + str(validation_date))

    config_path = f"example_validation_conf/job_configs/job_config_{group_id}.json"
    with open(config_path) as fp:
        job_config = json.load(fp)

    for table_config in job_config['configs']:
        table_config['args'][0] = f'--run_date={run_date}'
        table_config['args'][1] = f'--validation_date={validation_date}'

    print(f"Inside set_job_arguments_validations, "
          f"job_config --> {job_config}")

    with open(config_path, 'w') as fp:
        fp.write(json.dumps(job_config, indent=4))

