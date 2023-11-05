import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import datetime
from de_project7_help import input_paths3, save_overwrite,get_cities,get_city,get_city_info

# /usr/lib/spark/bin/spark-submit --master yarn --conf spark.executor.memory=1g --conf spark.executor.instances=2 --conf spark.dynamicAllocation.enabled=false --deploy-mode cluster /lessons/scripts/geo_city_info.py 2022-06-21 /user/alenakvon/data/geo/events /user/alenakvon/analytics/project/city_info
# /usr/lib/spark/bin/spark-submit --master yarn --conf spark.executor.memory=1g --conf spark.executor.instances=2 --conf spark.dynamicAllocation.enabled=false --deploy-mode cluster /lessons/scripts/geo_city_info.py 2022-06-21 /user/alenakvon/data/geo/events /user/alenakvon/analytics/project/city_info
def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    city_input_path = sys.argv[3]
    base_output_path = sys.argv[4]
    
    conf = SparkConf().setAppName(f"cityInfoJob-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    #read data    
    dt = datetime.datetime.strptime(date, '%Y-%m-%d') 
    paths = input_paths3(sql, base_input_path, date, -1*dt.month, 'message')

    messages = sql.read.parquet(*paths)    
    cities = get_cities(sql, city_input_path).cache()
    city_info = get_city_info(get_city(messages,cities))
   
    writer = save_overwrite(city_info)

    writer.save(f'{base_output_path}/date={date}')

if __name__ == "__main__":
    main()


# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/verified_tags_candidates.py 2022-05-31 5 300 /user/alenakvon/data/events /user/master/data/snapshots/tags_verified/actual /user/alenakvon/5.2.4/analytics/verified_tags_candidates_d5
