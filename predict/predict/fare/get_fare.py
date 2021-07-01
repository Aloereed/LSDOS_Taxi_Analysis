import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
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
    taxi = taxi.withColumn('PULocationID',taxi['PULocationID'].cast(IntegerType()))\
        .withColumn('DOLocationID',taxi['DOLocationID'].cast(IntegerType()))
    taxi = taxi.withColumn('trip_distance',taxi['trip_distance'].cast(FloatType()))
    taxi = taxi.withColumn('time',F.abs(F.unix_timestamp('tpep_pickup_datetime')-F.unix_timestamp('tpep_dropoff_datetime')))

    taxi.createOrReplaceTempView('taxi')

    train_sql = '''
    select time, trip_distance as distance, fare_amount as fare
    from taxi
    where PULocationID = 132 and time > 0 and trip_distance > 0 and fare_amount > 0
    '''
    fare_train = spark.sql(train_sql)
    fare_train.toPandas().drop(0).to_csv('fare_train.csv',header = True, index = False)

    test_sql = '''
    select time, trip_distance as distance, fare_amount as fare
    from taxi
    where PULocationID = 20 and time > 0 and trip_distance > 0 and fare_amount > 0
    '''
    fare_test = spark.sql(test_sql)
    fare_test.toPandas().drop(0).to_csv('fare_test.csv',header = True, index = False)