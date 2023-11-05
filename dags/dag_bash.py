import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime
 
path_events = '/user/alenakvon/data/geo/events'
path_city_info = '/user/alenakvon/analytics/project/city_info'
path_geo_info = '/user/alenakvon/analytics/project/geo_info'
path_user_recomendations = 'user/alenakvon/analytics/project/user_recomendations'
date = '2022-06-21'
#dt = datetime.datetime.strptime(date, '%Y-%m-%d') 
lag_month = -1 #* dt.month
city_input_path = "/user/alenakvon/data/spr/geo"

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
'owner': 'airflow',
'start_date':datetime(2022, 6, 21),
}

dag_spark = DAG(
dag_id = "datalake_de13_project7_etl",
default_args=default_args,
schedule_interval=None,
)

events_partitioned = SparkSubmitOperator(
task_id='events_geo_partitioned',
dag=dag_spark,
application ='../scripts/geo_partition_overwrite.py',
conn_id= 'yarn_spark',
application_args = ["2022-06-21", "/user/master/data/geo/events", path_events],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

# 2022-06-21 /user/alenakvon/data/geo/events /user/alenakvon/data/spr/geo /user/alenakvon/analytics/project/city_info
geo_city_info = SparkSubmitOperator(
task_id='geo_city_info',
dag=dag_spark,
application ='/home/alenakvon/scripts/geo_city_info.py',
conn_id= 'yarn_spark',
application_args = ["2022-06-21",  path_events, city_input_path, path_city_info],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

geo_info = SparkSubmitOperator(
task_id='geo_info',
dag=dag_spark,
application ='/home/alenakvon/scripts/geo_info.py',
conn_id= 'yarn_spark',
application_args = ["2022-06-21",  path_events, city_input_path, path_geo_info],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_recomendations = SparkSubmitOperator(
task_id='user_recomendations',
dag=dag_spark,
application ='/home/alenakvon/scripts/user_recomendations.py',
conn_id= 'yarn_spark',
application_args = ["2022-06-21",  path_events, city_input_path, path_user_recomendations],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

events_partitioned >> geo_city_info
geo_city_info >> [geo_info,user_recomendations]