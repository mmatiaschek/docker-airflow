from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from src.generate_rgbd import fuse_into_rgbd

default_args = {
	'start_date': days_ago(1)
}

with DAG(
    dag_id='omdena-proof-of-concept',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    task = PythonOperator(
        task_id='create-rgbd',
        provide_context=True,
        python_callable=fuse_into_rgbd,
        op_kwargs={'artifact': 'pc_1583742419-7h4mpmc587_1591451354739_100_000.pcd',
                   'base_data_path': '/usr/local/airflow/dags/data'
                   }
    )