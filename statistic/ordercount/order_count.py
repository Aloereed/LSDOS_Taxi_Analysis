import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark import  SparkConf
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
    taxi = taxi.withColumn('PULocationID',taxi['PULocationID'].cast(IntegerType()))\
        .withColumn('DOLocationID',taxi['DOLocationID'].cast(IntegerType()))

    taxi.createOrReplaceTempView('taxi')

    # 不同地点每小时出发订单变化
    hour_sql = '''
    select PULocationID, dayofyear(tpep_pickup_datetime) as puday, hour(tpep_pickup_datetime) as puhour, COUNT (*) as hour_count from taxi
    group by PULocationID,puday, puhour order by PULocationID,puday,puhour
    '''
    hourcount = spark.sql(hour_sql)
    hourcount = hourcount.groupby('PULocationID','puhour').agg(F.mean('hour_count').alias('mean_hour'),F.stddev('hour_count').alias('sd_hour')).orderBy('PULocationID','puhour')
    hourcount.toPandas().drop(0).to_csv('pu_hour.csv',header = True, index = False)

    # 不同地点每天（星期几）出发订单变化
    week_sql = '''
    select PULocationID, dayofweek(tpep_pickup_datetime) as puday,weekofyear(tpep_pickup_datetime) as puweek, COUNT (*) as day_count from taxi
    group by PULocationID, puweek,puday order by PULocationID,puday
    '''
    daycount = spark.sql(week_sql)
    daycount = daycount.groupby('PULocationID','puday').agg(F.mean('day_count').alias('mean_day'),F.stddev('day_count').alias('sd_day')).orderBy('PULocationID','puday')
    daycount.toPandas().drop(0).to_csv('pu_day.csv',header = True, index = False)

    # 不同地点每小时到达订单变化
    hour_sql = '''
    select DOLocationId, dayofyear(tpep_dropoff_datetime) as doday, hour(tpep_dropoff_dateTime) as dohour, COUNT (*) as hour_count from taxi
    group by DOLocationID,dohour,doday order by DOLocationID,dohour,doday
    '''
    hourcount = spark.sql(hour_sql)
    hourcount = hourcount.groupby('DOLocationID','dohour').agg(F.mean('hour_count').alias('mean_hour'),F.stddev('hour_count').alias('sd_hour')).orderBy('DOLocationID','dohour')
    hourcount.toPandas().drop(0).to_csv('do_hour.csv',header = True, index = False)

    # 不同地点每天（星期几）出发订单变化
    week_sql = '''
    select DOLocationID, dayofweek(tpep_dropoff_datetime) as doday,weekofyear(tpep_dropoff_datetime)  as doweek, COUNT (*) as day_count from taxi
    group by DOLocationID, doday,doweek order by DOLocationID,doday
    '''
    daycount = spark.sql(week_sql)
    daycount = daycount.groupby('DOLocationID','doday').agg(F.mean('day_count').alias('mean_day'),F.stddev('day_count').alias('sd_day')).orderBy('DOLocationID','doday')
    daycount.toPandas().drop(0).to_csv('do_day.csv',header = True, index = False)


