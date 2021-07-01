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
    taxi = taxi.withColumn('PULocationID',taxi['PULocationID'].cast(IntegerType()))\
        .withColumn('DOLocationID',taxi['DOLocationID'].cast(IntegerType()))

    taxi.createOrReplaceTempView('taxi')

    train_sql = '''
    select PULocationID, month(tpep_pickup_datetime) as pumonth, weekofyear(tpep_pickup_datetime) as puweek, dayofweek(tpep_pickup_datetime) as puday, hour(tpep_pickup_datetime) as puhour, COUNT (*) as hour_count 
    from taxi
    where PULocationID = 87 and mod(weekofyear(tpep_pickup_datetime), 4) != 2
    group by PULocationID, pumonth, puweek, puday, puhour
    order by PULocationID, pumonth, puweek, puday, puhour
    '''
    pickup_train = spark.sql(train_sql)
    pickup_train = pickup_train.select('PULocationID', 'pumonth', 'puday', 'puhour', 'hour_count')
    pickup_train.toPandas().drop(0).to_csv('pu_train.csv',header = True, index = False)

    test_sql = '''
    select PULocationID, month(tpep_pickup_datetime) as pumonth, weekofyear(tpep_pickup_datetime) as puweek, dayofweek(tpep_pickup_datetime) as puday, hour(tpep_pickup_datetime) as puhour, COUNT (*) as hour_count 
    from taxi
    where PULocationID = 87 and mod(weekofyear(tpep_pickup_datetime), 4) = 2
    group by PULocationID, pumonth, puweek, puday, puhour
    order by PULocationID, pumonth, puweek, puday, puhour
    '''
    pickup_test = spark.sql(test_sql)
    pickup_test = pickup_test.select('PULocationID', 'pumonth', 'puday', 'puhour', 'hour_count')
    pickup_test.toPandas().drop(0).to_csv('pu_test.csv',header = True, index = False)