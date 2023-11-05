import datetime
import sys
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pytz import all_timezones
r = 6371

def path_exists(spark, path):
    # spark is a SparkSession
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))

#/user/master/data/geo/events
def input_paths(date, depth): 
    dt = datetime.datetime.strptime(date, '%Y-%m-%d') 
    return [f"/user/alenakvon/data/geo/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]

def input_paths2(spark, base_path,date, lag_month): 
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    dt0 = get_start_date(dt,lag_month)
    return [f"{base_path}/date={(dt0+datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"\
            for x in range((dt - dt0).days+1)\
            if path_exists(spark,f"{base_path}/date={(dt0+datetime.timedelta(days=x)).strftime('%Y-%m-%d')}")]

def get_start_date(date, months=0):
    year = date.year
    month = date.month + months
    dyear, month = divmod(month - 1, 12)
    rdate = datetime.datetime(year + dyear, month + 1, 1)
    return rdate.replace(day = min(rdate.day, date.day))

# return list of paths for whole month
# lag_month = 0 is current month, "-1" - previous and current month
def input_paths3(spark, base_path,date, lag_month, etype): 
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    dt0 = get_start_date(dt,lag_month)
    return [f"{base_path}/date={(dt0+datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type={etype}"\
            for x in range((dt - dt0).days+1)\
            if path_exists(spark, f"{base_path}/date={(dt0+datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type={etype}")]

def input_paths_from_master(date, depth): 
    dt = datetime.datetime.strptime(date, '%Y-%m-%d') 
    return [f"/user/master/data/geo/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]

def save_overwrite(events):
    return events \
        .write \
        .mode('overwrite') \
        .format('parquet')

# find the nearest city
def get_city(df,cities):
    return df\
    .crossJoin(cities)\
    .withColumn('l1',\
        F.pow(\
            F.sin( ( F.radians(F.col('lat_c') )- F.radians(F.col('lat')) )/F.lit(2) )\
            , 2.0 ))\
    .withColumn('l2',\
        F.cos( F.radians(F.col('lat_c'))) * F.cos( F.radians(F.col('lat')))\
        *F.pow(\
            F.sin( ( F.radians(F.col('lon_c')) - F.radians(F.col('lon')) )/F.lit(2) )\
            , 2.0 ))\
    .withColumn('dist_m',\
        2*r*F.asin(F.sqrt(F.col('l1')+F.col('l2')))
               )\
    .withColumn("rid1", F.row_number().over(Window.partitionBy('event','lat','lon').orderBy(F.asc("dist_m"))))\
    .filter(F.col('rid1')==1)\
    .drop('lat_c','lon_c','dist_m','rid1','l1','l2')
    #.selectExpr(['event','lat','lon','city','timezone','date','event_type'])

# df - messages with city
def get_city_info(df):    
    # .filter(F.col('event.message_to').isNotNull())\
    # add date
    df = df.withColumn('date_ts', F.when(F.col('event.message_ts').isNotNull(),F.col('event.message_ts')).otherwise(F.col('event.datetime')))
    
    df_act_city = df\
        .withColumn("rid", F.row_number().over(Window.partitionBy(F.col('event.message_from')).orderBy(F.desc("date_ts"))))\
        .filter(F.col('rid')==1)\
        .withColumn("TIME_UTC",F.date_format(F.col('date_ts'),'HH:mm:ss'))\
        .withColumn("localtime",F.when(F.col('timezone').isin(all_timezones),\
                               F.date_format(F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone')),'HH:mm:ss'))\
        .otherwise(None))\
        .selectExpr(['event.message_from as user_id','city as act_city','localtime'])
    
    #Добавим признаки город предыдущий, город следующий
    # тогда последовательность городов будет непрерывна если: текущий город либо равен предыдущем, либо равен следующему
    # текущий \ пред  \ след   \ тек=пред \ тек=след \ объеден
    # Питер  \ Питер  \ Москва \ True     \ False    \ True
    # Москва \ Питер  \ Москва \ False    \ False    \ True
    # Москва \ Москва \ Москва \ True     \ True     \ True
    # Москва \ Москва \ Москва \ True     \ True     \ True
    # Москва \ Москва \ Казань \ True     \ True     \ True
    # Казань \ Москва \ Москва \ False    \ False    \ False
    # Москва \ Казань \ Тверь  \ True     \ False    \ False
    # Тверь  \ Москва \ Тверь  \ False    \ True     \ True
    # Тверь  \ Тверь  \ Тверь  \ True     \ True     \ True
    #
    # однодневные посещения не фильтруются
    # фильтруются только непрерывная последовательность
    # и чтобы взять только крайние города, добавил + условие что след<>пред
    df_travels_city = df\
    .withColumn("city_next", F.lead('city').over(Window.partitionBy('event.message_from').orderBy(F.asc("date_ts"),F.asc("event.message_id"))))\
    .fillna('na')\
    .filter(F.col('city') != F.col('city_next'))\
    .withColumn('date_pre', F.lead('date_ts').over(Window.partitionBy('event.message_from').orderBy(F.desc("date_ts"))))\
    .withColumn('days',F.datediff('date_ts','date_pre'))\
    .drop('city_next','date_pre')\
    
    df_home_city = df_travels_city\
    .filter(F.col('days')>=26)\
    .withColumn("rid2", F.row_number().over(Window.partitionBy('event.message_from').orderBy(F.desc("date_ts"))))\
    .filter(F.col('rid2')==1)\
    .selectExpr(['event.message_from as user_id','city as home_city'])
    
    df_trips= df_travels_city\
    .selectExpr(['event.message_from as user_id']).groupBy('user_id').agg(F.expr("count(1) as travel_count"))
    
    #travel_array
    df_travel= df_travels_city\
    .orderBy(F.asc('date_ts'))\
    .selectExpr(['event.message_from as user_id','city']).groupBy('user_id').agg(F.collect_list("city").alias("travel_array"))
 
    return df_act_city.join(df_home_city,'user_id','left').join(df_trips,'user_id','left').join(df_travel,'user_id','left')
 
# /user/alenakvon/data/spr/geo
def get_cities(spark, path_geo):
    r = 6371
    geo = spark.read.csv(path_geo,sep=";", inferSchema=True, header=True,)
    cities = geo.withColumn("timezone",F.concat(F.lit("Australia/"), F.col('city')))\
    .selectExpr(['city','lat as lat_c','lon as lon_c','timezone'])

    city1 = cities\
    .withColumn("isTimezone",F.when(F.col('timezone').isin(all_timezones),1).otherwise(0))\
    .filter(F.col('isTimezone')==1)\
    .drop('isTimezone')

    return cities\
        .withColumn("isTimezone",F.when(F.col('timezone').isin(all_timezones),1).otherwise(0))\
        .selectExpr(['city','lat_c as lat','lon_c as lon'])\
        .filter(F.col('isTimezone')==0)\
        .crossJoin(city1.drop('city','isTimezone'))\
        .withColumn('l1',\
            F.pow(\
                F.sin( ( F.radians(F.col('lat_c') )- F.radians(F.col('lat')) )/F.lit(2) )\
                , 2.0 ))\
        .withColumn('l2',\
            F.cos( F.radians(F.col('lat_c'))) * F.cos( F.radians(F.col('lat')))\
            *F.pow(\
                F.sin( ( F.radians(F.col('lon_c')) - F.radians(F.col('lon')) )/F.lit(2) )\
                , 2.0 ))\
        .withColumn('dist_m',\
            2*r*F.asin(F.sqrt(F.col('l1')+F.col('l2')))
                   )\
        .withColumn("rid1", F.row_number().over(Window.partitionBy('city','lat','lon').orderBy(F.asc("dist_m"))))\
        .filter(F.col('rid1')==1)\
        .selectExpr(['city','lat as lat_c','lon as lon_c','timezone'])\
        .union(city1)

def get_geo_info_by_month_by_week(spark, path, date, lag_month, etype, cities):
    # get list of paths for whole month, path was skipped if not exists
    if etype != 'registration':
        paths = input_paths3(spark, path, date, lag_month, etype)
    else:
        paths = input_paths3(spark, path, date, lag_month, 'message')
        
    if len(paths)>0:
        df = spark.read.parquet(*paths)
    else:
        print("paths not found...")
        sys.exit(0)
        
    if etype == 'reaction' or etype == 'subscription':
        df = df\
            .withColumn('date', F.col('event.datetime'))
    
    #.filter(F.col('event.message_to').isNotNull())\                   
    if etype == 'message':
        df = df\
            .withColumn('date', F.when(F.col('event.message_ts').isNotNull(),F.col('event.message_ts')).otherwise(F.col('event.datetime')))
        
    if etype == 'registration':        
        df= df\
            .withColumn('date', F.when(F.col('event.message_ts').isNotNull(),F.col('event.message_ts')).otherwise(F.col('event.datetime')))\
            .withColumn("rid", F.row_number().over(Window.partitionBy(F.col('event.message_from')).orderBy(F.asc("date"))))\
            .filter(F.col('rid')==1)\
            .drop('rid')
          
    window_month = Window().partitionBy('month','zone_id')   

    return get_city(df,cities)\
        .withColumn('week', F.date_format(F.col('date'), 'W'))\
        .withColumn('month', F.date_format(F.col('date'), 'M'))\
        .selectExpr(['month','week','city as zone_id'])\
        .withColumn("month_" + etype,F.count(F.lit(1)).over(window_month))\
        .groupBy("month",'week','zone_id','month_' + etype)\
        .agg(F.expr('sum(1) as week_' + etype))



#path_events = '/user/alenakvon/data/geo/events'
#path_city_info = '/user/alenakvon/analytics/project/city_info'
#date = '2022-06-21'
#dt = datetime.datetime.strptime(date, '%Y-%m-%d') 
#lag_month = -1 #* dt.month
#city_input_path = "/user/alenakvon/data/spr/geo"
#cities = get_cities(spark, city_input_path).cache()

def get_recomend(spark, path_events, path_city_info, date,lag_month,cities):    
    #messages
    paths = input_paths3(spark, path_events ,date, lag_month, 'message')
    if len(paths)==0:
        print("paths not found...")
        sys.exit(0)
        
    msgs = spark.read.parquet(*paths)\
    .filter(F.col('event.message_to').isNotNull())\
    .selectExpr(['event.message_from as user_from','event.message_to as user_to']).distinct()
    
    msgs = msgs.union(msgs.selectExpr(['user_to','user_from'])).distinct().orderBy('user_from','user_to')
    
    # users info
    paths = f"{path_city_info}/date={date}"
    if not(path_exists(spark,paths)):
        print("user info paths not found...")
        sys.exit(0)
        
    ci = spark.read.parquet(paths)
    
    # get user and channel
    paths = input_paths3(spark, path_events ,date, lag_month, 'subscription')
    
    if len(paths)==0:
        print("paths not found...")
        sys.exit(0)
        
    #paths = input_paths3(spark, '/user/alenakvon/data/geo/events',date, -1*dt.month, 'subscription')
    subs = spark.read.parquet(*paths)\
    .selectExpr(['event.user','event.subscription_channel']).distinct()
    subs_users = subs.join(subs.withColumnRenamed("user","user_right"),"subscription_channel","inner")\
    .filter(F.col('user')!=F.col('user_right')).drop('subscription_channel').orderBy('user','user_right')
    
    return subs_users\
    .join(ci,subs_users.user == ci.user_id,"inner")\
    .selectExpr(['user as user_left','user_right','lat as lat_left','lon as lon_left'])\
    .join(ci,subs_users.user_right == ci.user_id,"inner")\
    .selectExpr(['user_left','user_right','lat_left','lon_left','lat as lat_right','lon as lon_right'])\
    .withColumn('l1',\
        F.pow(\
            F.sin( ( F.radians(F.col('lat_left') )- F.radians(F.col('lat_right')) )/F.lit(2) )\
            , 2.0 ))\
    .withColumn('l2',\
        F.cos( F.radians(F.col('lat_left'))) * F.cos( F.radians(F.col('lat_right')))\
        *F.pow(\
            F.sin( ( F.radians(F.col('lon_left')) - F.radians(F.col('lon_right')) )/F.lit(2) )\
            , 2.0 ))\
    .withColumn('dist_m',\
        2*r*F.asin(F.sqrt(F.col('l1')+F.col('l2')))
               )\
    .filter(F.col('dist_m')<1.0)\
    .drop('l1','l2','lat_left','lon_left','lat_right','lon_right','dist_m')\
    .join(msgs,[F.col('user_left')== msgs.user_from,F.col('user_right')== msgs.user_to],"left")\
    .filter(F.col('user_to').isNull())\
    .drop('user_from','user_to')\
    .join(ci,F.col('user_left') == ci.user_id,'inner')\
    .withColumn("processed_dttm", F.current_timestamp())\
    .selectExpr(['user_left','user_right','processed_dttm', 'act_city as zone_id','localtime'])
