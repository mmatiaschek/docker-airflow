from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from src.generate_rgbd import fuse_into_rgbd
from src.pcd_processing import reduce_bounding_box, rotate_pcd

default_args = {
	'start_date': days_ago(1)
}

with DAG(
    dag_id='omdena-proof-of-concept',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    create_rgbd = PythonOperator(
        task_id='create-rgbd',
        provide_context=True,
        python_callable=fuse_into_rgbd,
        op_kwargs={'artifact': 'pc_1583742419-7h4mpmc587_1591451354739_100_000.pcd',
                   'base_data_path': '/usr/local/airflow/dags/data'
                   }
    )
    
    reduce_bb = PythonOperator(
        task_id='reduce-bounding-box',
        provide_context=True,
        python_callable=reduce_bounding_box,
        op_kwargs={'artifact': 'pc_1583742419-7h4mpmc587_1591451354739_100_000.pcd',
                   'base_data_path': '/usr/local/airflow/dags/data'
                   }
    )
    
    rotate_pcd = PythonOperator(
        task_id='rotate-pcd',
        provide_context=True,
        python_callable=rotate_pcd,
        op_kwargs={'artifact': 'pc_1583742419-7h4mpmc587_1591451354739_100_000.pcd',
                   'base_data_path': '/usr/local/airflow/dags/data'
                   }
    )
    
    reduce_bb >> rotate_pcd