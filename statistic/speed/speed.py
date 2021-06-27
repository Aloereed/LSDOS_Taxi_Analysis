import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import  SparkConf
from pyspark.sql.types import FloatType
if __name__ == '__main__':
    spark = (SparkSession
                .builder
                .appName('dist_loc')
                .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                .enableHiveSupport()
                .getOrCreate()
                )
    hive_database = "taxi"
    hive_table = "trips"
    hive_read = "select * from  {}.{}".format(hive_database, hive_table)
    taxi = spark.sql(hive_read) # 读入数据
    
    # taxi = spark.read.csv('Taxidata_20190501_20190602.csv', header = True) # 读入数据
    taxi = taxi.withColumn('trip_distance',taxi['trip_distance'].cast(FloatType()))
    taxi = taxi.withColumn('speed',3600*taxi['trip_distance']/F.abs(F.unix_timestamp('tpep_pickup_datetime')-F.unix_timestamp('tpep_dropoff_datetime')))\
                .withColumn('mediantime',F.to_timestamp(F.unix_timestamp('tpep_pickup_datetime')+F.abs(F.unix_timestamp('tpep_pickup_datetime')-F.unix_timestamp('tpep_pickup_datetime'))/2)) # 取中间时刻作为估计

    taxi.createOrReplaceTempView('taxi')
    
    # # 每小时平均速度均值和标准差
    # hour_sql = '''
    # select hour(mediantime) as sp_hour, dayofyear(mediantime) as sp_day, AVG(speed) as avg_speed from taxi
    # group by sp_hour, sp_day order by sp_hour, sp_day
    # '''
    # hourspeed = spark.sql(hour_sql)
    # hourspeed = hourspeed.groupby('sp_hour').agg(F.mean('avg_speed').alias('mean_speed'),F.stddev('avg_speed').alias('sd_speed')).orderBy('sp_hour')
    # hourspeed.toPandas().drop(0).to_csv('hourspeed.csv',header = True, index = False)


    # # 每天平均速度均值和标准差
    # day_sql = '''
    #     select dayofweek(mediantime) as sp_day, weekofyear(mediantime) as sp_week, AVG(speed) as avg_speed from taxi
    #     group by sp_day, sp_week order by sp_day, sp_week
    #     '''
    # dayspeed = spark.sql(day_sql)
    # dayspeed = dayspeed.groupby('sp_day').agg(F.mean('avg_speed').alias('mean_speed'),F.stddev('avg_speed').alias('sd_speed')).orderBy('sp_day')
    # dayspeed.toPandas().drop(0).to_csv('dayspeed.csv',header = True, index = False)


    # # 每个出发地理位置每个小时速度
    # sql = '''
    # select PULocationID, hour(tpep_pickup_datetime) as puhour, AVG(speed) as avg_speed, STDDEV(speed) as sd_speed from taxi group by PULocationID, puhour order by PULocationID, puhour
    # '''
    # speed = spark.sql(sql)
    # speed.toPandas().drop(0).to_csv('hourloc_speed.csv',header = True, index = False)

    # 每个小时每条路线之间速度
    route_sql = '''
    select PULocationID, DOLocationID, hour(tpep_pickup_datetime) as puhour, AVG(speed) as avg_speed, STDDEV(speed) as sd_speed from taxi group by PULocationID,DOLocationID,  puhour order by PULocationID, DOLocationID, puhour
    '''
    route_speed = spark.sql(route_sql)
    route_speed.toPandas().drop(0).to_csv('route_speed.csv',header = True, index = False)
