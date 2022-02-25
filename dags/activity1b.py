import csv 
import json 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime

from sqlalchemy import JSON


default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime(2022, 2, 23),
	'retries': 0
}

def converter(csvFilePath, jsonFilePath):
    jsonArray = []
      
 
    with open(csvFilePath, encoding='utf-8') as csvf: 
        
        csvReader = csv.DictReader(csvf) 

       
        for row in csvReader: 
            
            jsonArray.append(row)
  
    
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)
          
csvFilePath = r'/home/adminhp/lowercase/sample.csv'
jsonFilePath = r'/home/adminhp/uppercase/file.json'
converter(csvFilePath, jsonFilePath)


dag = DAG(dag_id='activity1b', default_args=default_args, start_date=datetime(2022, 2, 23), schedule_interval= '*/1 * * * *', catchup=False)

start = DummyOperator(task_id='start', dag=dag)

convert = PythonOperator(task_id='convert', python_callable= converter, dag=dag)

start >> convert

# globals()['activity1b'] = dag






    