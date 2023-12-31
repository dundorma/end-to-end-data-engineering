from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
sys.path.insert(0,"/opt/airflow")
import etl.extract_air_quality as extract_air_quality
import etl.extract_traffic_jam as extract_traffic_jam
import etl.transform_air_quality as transform_air_quality
import etl.transform_traffic_jam as transform_traffic_jam
import etl.load as etl_load
import etl.cleanup as cleanup

etl_dag = DAG(
    dag_id = "etl_dag",
    default_args={"start_date": "2023-11-25"},
    schedule="0 0 */3 * *"
)

extract_air_task = PythonOperator(
    task_id="e_air",
    python_callable=extract_air_quality.main,
    dag=etl_dag
)

extract_traffic_task = PythonOperator(
    task_id="e_traffic",
    python_callable=extract_traffic_jam.main,
    dag=etl_dag
)

transform_air_task = PythonOperator(
    task_id="t_air",
    python_callable=transform_air_quality.main,
    dag=etl_dag
)

transform_traffic_task = PythonOperator(
    task_id="t_traffic",
    python_callable=transform_traffic_jam.main,
    dag=etl_dag
)

load_task = PythonOperator(
    task_id="load_to_psql",
    python_callable=etl_load.main,
    dag=etl_dag
)

cleanup_task = PythonOperator(
    task_id="cleanup_task",
    python_callable=cleanup.main,
    dag=etl_dag
)

extract_traffic_task >> transform_traffic_task >> extract_air_task
extract_air_task >> transform_air_task >> load_task

load_task >> cleanup_task
