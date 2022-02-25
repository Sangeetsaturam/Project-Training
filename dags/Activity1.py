from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime


default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime(2022, 2, 23),
	'retries': 0
}


def Activity1():
    with open('/home/adminhp/lowercase/lowercase.txt', 'r') as a:
        y = a.read().upper()
    with open('/home/adminhp/uppercase/uppercase.txt', 'a') as b:
        b.write(y)


dag = DAG(dag_id='activity1', default_args=default_args, start_date=datetime(2022, 2, 23), schedule_interval= '*/2 * * * *', catchup=False)

start = DummyOperator(task_id='start', dag=dag)

convert = PythonOperator(task_id='conversion', python_callable=Activity1, dag=dag)

start >> convert

globals()['Activity1'] = dag