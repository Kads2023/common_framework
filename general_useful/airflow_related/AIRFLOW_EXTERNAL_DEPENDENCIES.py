import datetime as dt
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from hrz_polariswju.hrz_edlxp_adshn.utils.job_arguments import set_job_arguments
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.task_group import TaskGroup
from hrz_polariswju.hrz_edl_admdw.utils.iris_alerts import alert_to_iris

dag_name = "DAG_NAME"
schedule_interval_mins = 1440
local_tz = pendulum.timezone("America/Los_Angeles")


def get_most_recent_dag_run(dte):
    dag_runs = DagRun.find(dag_id="DAG_NAME")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    print(f"dag_runs --> {dag_runs}")
    if dag_runs:
        print(f"dag_runs[1].execution_date --> {dag_runs[1].execution_date}")
        return dag_runs[1].execution_date


default_args = {
    'owner': 'hrz_polariswju',
    'depends_on_past': True,
    'start_date': datetime(2023, 5, 8, 0, tzinfo=local_tz),
    'email': ['wisdom-edw-alerts@paypal.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    'run_as_user': '<>',
    'on_failure_callback': alert_to_iris,
    'queue': 'hrz_edl_admdw',
    'pool': 'hrz_edl_admdw',
    'wait_for_downstream': True
}

bash_command = '''
. /.profile;
python3 <>
'''

with DAG(dag_name,
         default_args=default_args,
         schedule_interval='0 1 */1 * *',
         catchup=True,
         max_active_runs=1,
         tags=[]
         ) as dag:

    with TaskGroup("DAG_NAME") as DAG_NAME:
        with TaskGroup("TASK_GROUP_NAME") as TASK_GROUP_NAME:
            WAIT_FOR_PREV_RUN_COMPLETION = ExternalTaskSensor(
                task_id='WAIT_FOR_PREV_RUN_COMPLETION',
                external_dag_id='DAG_NAME',
                external_task_ids=[
                    'DAG_NAME.TASK_GROUP_NAME.TASK_GROUP_NAME_MRG'],
                execution_date_fn=get_most_recent_dag_run,
                mode='reschedule',
                poke_interval=60 * 5,  # 5 minutes
                timeout=60 * 30  # 30 minutes
            )

            # This or above one
            # WAIT_FOR_PREV_COMPLETION = ExternalTaskSensor(
            #     task_id='WAIT_FOR_PREV_COMPLETION',
            #     external_dag_id='DAG_NAME',
            #     external_task_ids=[f'DAG_NAME.TASK_GROUP_NAME.PPAS_SET_JOB_ARGUMENTS',
            #                        f'DAG_NAME.TASK_GROUP_NAME.TASK_GROUP_NAME_MRG'],
            #     execution_delta=dt.timedelta(hours=1),
            #     mode='reschedule',
            #     poke_interval=60 * 5,  # 5 minutes
            #     timeout=60 * 30  # 30 minutes
            # )

            PPAS_SET_JOB_ARGUMENTS = PythonOperator(
                task_id='PPAS_SET_JOB_ARGUMENTS',
                provide_context=True,
                python_callable=set_job_arguments,
                op_kwargs={'local_tz': local_tz, 'schedule_interval_mins': schedule_interval_mins}
            )

            TASK_GROUP_NAME_1_INS = BashOperator(
                task_id='TASK_GROUP_NAME_1_INS',
                bash_command=bash_command % dict(
                    parent_dag_name='DAG_NAME',
                    table_name='WRK_TASK_GROUP_NAME_1',
                    application_system_name='<>',
                    batch_job_name='WRK_TASK_GROUP_NAME_1_DELTA_LOAD'
                )
            )

            TASK_GROUP_NAME_3_INS = BashOperator(
                task_id='TASK_GROUP_NAME_3_INS',
                bash_command=bash_command % dict(
                    parent_dag_name='DAG_NAME',
                    table_name='WRK_TASK_GROUP_NAME_3',
                    application_system_name='<>',
                    batch_job_name='WRK_TASK_GROUP_NAME_3_DELTA_LOAD'
                )
            )

            TASK_GROUP_NAME_MRG = BashOperator(
                task_id='TASK_GROUP_NAME_MRG',
                bash_command=bash_command % dict(
                    parent_dag_name='DAG_NAME',
                    table_name='TASK_GROUP_NAME',
                    application_system_name='<>',
                    batch_job_name='TASK_GROUP_NAME_LOAD'
                )
            )

        WAIT_FOR_PREV_RUN_COMPLETION >> PPAS_SET_JOB_ARGUMENTS \
        >> TASK_GROUP_NAME_1_INS \
        >> TASK_GROUP_NAME_3_INS \
        >> TASK_GROUP_NAME_MRG

DAG_NAME