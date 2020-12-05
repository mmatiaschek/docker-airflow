""""
Proof of concept: DAG using http operator and integration with Azure.

Note that interaction with API endpoints can also be done using the requests library in a PythonOperator, for more
complex workflows.

We use the postman echo example as a demonstration: https://docs.postman-echo.com/

relevant airflow docs:
- https://airflow.apache.org/docs/stable/_api/airflow/operators/http_operator/index.html
- https://airflow.apache.org/docs/stable/integration.html
"""


from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.contrib.sensors.python_sensor import PythonSensor
import json
import logging

pipeline_token = Variable.get('azure_pipeline_token')
face_detection_pipeline_endpoint = '7d2a3b9b-a37e-4e61-a9d7-6da6affc3ba1'
face_blurring_pipeline_endpoint = 'c8605226-2272-40e0-80d6-858df48889fe'

def wait_till_pipeline_end(task_xcom, experiment_name, *args, **kwargs):
    from azureml.pipeline.core.run import PipelineRun
    from azureml.core import Experiment
    from azureml.core import Workspace
    ws = Workspace.from_config(path='./dags/azureml/config.json')

    run_id = task_xcom.split('{}')[2].split(':')[8].split('/')[6].split('?')[0]
    pipeline_run = PipelineRun(
        experiment=Experiment(ws, experiment_name),
        run_id=run_id
    )
    status = pipeline_run.get_status()
    print(experiment_name, run_id, status)
    return status != 'Running'


default_args = {
    'start_date': days_ago(1)  # otherwise waits until tonight to be scheduled
}

with DAG(
        dag_id='omdena-http-pipelines-proof-of-concept',
        default_args=default_args,
        schedule_interval=None,
) as dag:

    detect_face = SimpleHttpOperator(
            task_id='face_detection_haar_cascade',
            endpoint=face_detection_pipeline_endpoint,
            http_conn_id='azure_pipelines_http_endpoint',
            method='POST',
            headers={
                'Authorization': 'Bearer ' + pipeline_token,
                'Content-Type': 'application/json'},
            data=json.dumps({
                "ExperimentName": "Face_detection_Haar_cascade_pipeline_REST",
                "RunSource": "SDK",
                "ParameterAssignments": {"sample_num": "1"}}),
            log_response=True,
            xcom_push=True
    )

    wait_face_detection_pipeline = PythonSensor(
            task_id='sense_face_detection_pipeline_end',
            poke_interval=10,
            timeout=60*10,  # 10 minutes
            python_callable=wait_till_pipeline_end,
            op_kwargs={
                'experiment_name': 'Face_detection_Haar_cascade_pipeline_REST',
                'task_xcom': "{{ ti.xcom_pull(task_ids='face_detection_haar_cascade', key='return_value') }}"
                }
    )

    blur_face = SimpleHttpOperator(
            task_id='face_blurring',
            endpoint=face_blurring_pipeline_endpoint,
            http_conn_id='azure_pipelines_http_endpoint',
            method='POST',
            headers={
                'Authorization': 'Bearer ' + pipeline_token,
                'Content-Type': 'application/json'},
            data=json.dumps({
                "ExperimentName": "Face_detection_Haar_cascade_pipeline_REST",
                "RunSource": "SDK"}),
            log_response=True,
            xcom_push=True
    )

    detect_face >> wait_face_detection_pipeline >> blur_face



