import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from de_project7_help import input_paths2, save_overwrite

#/user/master/data/geo/events /user/alenakvon/data/geo/events
def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    conf = SparkConf().setAppName(f"GeoEventsPartitioningJob-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    paths = input_paths2(sql, base_input_path,date, -1)    
    events = sql.read.parquet(*paths)

    writer = save_overwrite(events)

    writer.partitionBy("date", "event_type").save(f'{base_output_path}/date={date}')

if __name__ == "__main__":
    main()




# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/partition_overwrite.py 2022-05-04 5 /user/vasilyevol/data/events /user/vasilyevol/analytics/user_interests_d5_TEST