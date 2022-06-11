import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
SPARK_STEPS = [
    {
        'Name': 'poke',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
#                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--num-executors', '2',
                '--driver-memory', '512m',
                '--executor-memory', '3g',
                '--executor-cores', '2',
                '--py-files','s3://shawn-for-spark/noChange.zip', 's3://shawn-for-spark/workflow_entry.py',
#                's3://<your_bucket>/spark-engine_2.12-0.01.jar',
                '-p', "{'input_path': 's3://mydata-convert/landing/source.csv', 'name':'midterm', 'file_type':'txt', 'output_path': 's3://mydata-convert/source/', 'partition_column':'catelogue'}"
#                '-o', 'parquet',
#                '-p', 'midterm-project',
#                '-i', 'Csv',
#                '-s', "{{ task_instance.xcom_pull('parse_request', key='s3location') }}",
#                '-d', 's3://midterm-output-landing/pokedex-2022',
#                '-c', 'type_1',
#                '-m', 'append',
#                '--input-options', 'header=true'
            ]
        }
    }
]
CLUSTER_ID = "j-3MFEKYXGZPJD7"
DEFAULT_ARGS = {
    'owner': 'Shawn Gao',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['shawnloveca@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}
def retrieve_s3_files(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location']
    kwargs['ti'].xcom_push(key = 's3location', value = s3_location)
dag = DAG(
    'source_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None
)
pipeline_start = DummyOperator(
    task_id = 'Pipeline_start',
    dag = dag
)
parse_request = PythonOperator(
    task_id = 'Parse_request',
    provide_context = True,
    python_callable = retrieve_s3_files,
    dag=dag
)
step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = CLUSTER_ID,
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)
step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = CLUSTER_ID,
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id = "aws_default",
    dag = dag
)
pipeline_finished = DummyOperator(
    task_id = 'Pipeline_finished',
    dag = dag
)
pipeline_start >> parse_request >> step_adder >> step_checker >> pipeline_finished

