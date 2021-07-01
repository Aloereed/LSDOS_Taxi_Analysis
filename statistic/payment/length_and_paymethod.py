import findspark
findspark.init()

from pyspark import  SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os

spark = (SparkSession
         .builder
         .appName('example-pyspark-read-and-write-from-hive')
         .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
         .enableHiveSupport()
         .getOrCreate()
         )

# df = spark.read.csv("sampled_data.csv", header=True)
df = spark.sql("select * from taxi.trips where isNotNull(vendorid)")
df = df.filter("passenger_count>0 and trip_distance>0 and fare_amount>0 and total_amount>0")


res = df.groupby('payment_type').agg(F.mean(df.trip_distance).alias("avg(dist)"), F.variance(df.trip_distance).alias("var(dist)"), F.count(df.trip_distance).alias("count(dist)"))
if os.path.exists('paytype_dist.csv'): os.remove('paytype_dist.csv')
res.sort(df.payment_type.asc()).toPandas().to_csv('paytype_dist.csv',header = True, index = False)