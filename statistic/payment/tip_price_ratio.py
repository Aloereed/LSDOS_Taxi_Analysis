import findspark
findspark.init()

from pyspark import  SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, FloatType

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
df = df.filter("passenger_count>0 and trip_distance>0 and fare_amount>0 and total_amount>0 and trip_distance<40000 and trip_distance>0")
df = df.withColumn('tip_amount', df["tip_amount"].cast(FloatType()))
df = df.withColumn('total_amount', df["total_amount"].cast(FloatType()))

def cutme_prototype(value:float, splits) -> float:
    '''Categorize the given value.'''
    # tags   = ()
    ind = 0

    value = float(value)
    while ind < len(splits):
        if value > splits[ind]:
            ind += 1
        else:
            break

    return ind

@udf(returnType=IntegerType())
def cut_ratio(value):
    splits = tuple(i/100 for i in range(0, 51, 1))
    return cutme_prototype(value, splits)

@udf
def label_ratio(rate):
    splits = tuple(i/100 for i in range(0, 51, 1))
    tags = ["={}".format(splits[0])]
    for i in range(len(splits) - 1):
        tags.append("{}~{}".format(splits[i], splits[i+1]))
    tags.append(">{}".format(splits[-1]))
    return tags[rate]




#%% 小费与总价比值的频次统计表

# tip_ratio = df.select((df.tip_amount/df.total_amount).alias("ratio"), F.lit(1))
tip_ratio_rate = df.select(label_ratio(cut_ratio(df.tip_amount/df.total_amount)).alias("ratio"), F.lit(1).alias("count"))\
    .groupBy("ratio").agg(F.sum("count"))
# res = total_tip.groupby("total").agg(F.mean("tip"), F.variance("tip"), F.count("tip"))
#
# res.sort("total").toPandas().to_csv('price_tip_stat.csv',header = True, index = False)
# print("success")

#%%
tip_ratio_rate.sort("ratio").toPandas().to_csv("tip_ratio_rate.csv", header=True, index=False)

