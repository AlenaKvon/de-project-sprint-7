import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import datetime
from de_project7_help import save_overwrite,get_cities,get_geo_info_by_month_by_week, get_recomend
#path_events = '/user/alenakvon/data/geo/events'
#path_city_info = '/user/alenakvon/analytics/project/city_info'
#date = '2022-06-21'
#dt = datetime.datetime.strptime(date, '%Y-%m-%d') 
#lag_month = -1 #* dt.month
def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    city_input_path = sys.argv[3]
    base_output_path = sys.argv[4]
    
    conf = SparkConf().setAppName(f"cityInfoJob-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    sql.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    #read data    
    cities = get_cities(sql, city_input_path).cache()
    
    dt = datetime.datetime.strptime(date, '%Y-%m-%d') 

    res= get_recomend(sql, base_input_path, city_input_path, date,-1*dt.month,cities)

    writer = save_overwrite(res)

    writer.partitionBy("user_left").save(f'{base_output_path}')

if __name__ == "__main__":
    main()


# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/verified_tags_candidates.py 2022-05-31 5 300 /user/alenakvon/data/events /user/master/data/snapshots/tags_verified/actual /user/alenakvon/5.2.4/analytics/verified_tags_candidates_d5
