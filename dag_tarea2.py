from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'javier_lopez',
    'start_date': datetime(2020, 5, 20, 11, 0, 0)
}


def frase1():
    frase1 = "El uso de Airflow en la universidad de Springfield"
    lista1 = frase1.split()
    for palabra in lista1:
        print(palabra)

def frase2():
    frase2 = "El otro día mi hija me dijo que Airflow no se utilizaba en la universidad de Springfield, y yo le dije: qué no Lisa? qué no?"
    lista2 = frase2.split()
    for palabra in lista2:
        print(palabra)

def pudflan():
    frase3 = "Púdrete Flanders"
    
    for i in range(150):
        print(frase3)

with DAG('dag_tarea2',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    start = DummyOperator(task_id='start')

    frase1_python = PythonOperator(task_id='frase1_python',
                                   python_callable=frase1)

    frase2_python = PythonOperator(task_id='frase2_python',
                                   python_callable=frase2)

    pudflan_python = PythonOperator(task_id='pudrete_flanders_python',
                                   python_callable=pudflan)

start >> frase1_python >> frase2_python >> pudflan_python