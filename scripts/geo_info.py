import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import datetime
from de_project7_help import save_overwrite,get_cities,get_geo_info_by_month_by_week

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
    df_subs = get_geo_info_by_month_by_week(sql,base_input_path, date, -1, 'subscription',cities)
    df_reas = get_geo_info_by_month_by_week(sql,base_input_path, date, -1, 'reaction',cities)
    df_msgs = get_geo_info_by_month_by_week(sql,base_input_path, date, -1, 'message',cities)
    df_regs = get_geo_info_by_month_by_week(sql,base_input_path, date, -1, 'registration',cities)

    geo_info = df_subs.join(df_reas,['month','week','zone_id'],"full_outer")\
        .join(df_msgs,['month','week','zone_id'],"full_outer")\
        .join(df_regs,['month','week','zone_id'],"full_outer")
  
    writer = save_overwrite(geo_info)

    writer.partitionBy("month").save(f'{base_output_path}')

if __name__ == "__main__":
    main()


# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/verified_tags_candidates.py 2022-05-31 5 300 /user/alenakvon/data/events /user/master/data/snapshots/tags_verified/actual /user/alenakvon/5.2.4/analytics/verified_tags_candidates_d5
