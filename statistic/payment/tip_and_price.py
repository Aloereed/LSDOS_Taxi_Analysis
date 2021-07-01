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
df = df.withColumn('tip_amount',df['tip_amount'].cast(FloatType()))

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
def cut_total(value):
    splits = (0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70)
    return cutme_prototype(value, splits)

@udf
def label_total(rate):
    splits = (0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70)
    tags = ["={}".format(splits[0])]
    for i in range(len(splits) - 1):
        tags.append("{}~{}".format(splits[i], splits[i+1]))
    tags.append(">{}".format(splits[-1]))
    return tags[rate]

@udf(returnType=IntegerType())
def cut_tip(value):
    splits = tuple(i/2 for i in range(-1, 60, 1))
    return cutme_prototype(value, splits)

@udf
def label_tip(rate):
    splits = tuple(i/2 for i in range(-1, 60, 1))
    tags = ["<{}".format(splits[0])]
    for i in range(len(splits) - 1):
        tags.append("{}~{}".format(splits[i], splits[i+1]))
    tags.append(">{}".format(splits[-1]))
    tags[1] = "=0.0"
    return tags[rate]



#%% 统计小费关于总价的性质，即均值和方差
total_tip = df.select(label_total(cut_total('total_amount')).alias("total"), df.tip_amount.alias("tip"))
res = total_tip.groupby("total").agg(F.mean("tip"), F.variance("tip"), F.count("tip"))
if os.path.exists('price_tip_stat.csv'): os.remove('price_tip_stat.csv')
res.sort("total").toPandas().to_csv('price_tip_stat.csv',header = True, index = False)
print("success")

#%% 统计频率表，用于画直方图
total_tip_data = df.select(label_total(cut_total('total_amount')).alias("total"), label_tip(cut_tip("tip_amount")).alias("tip"))
data = total_tip_data.crosstab("total", "tip")

if os.path.exists('price_tip_data.csv'): os.remove('price_tip_data.csv')
data.toPandas().to_csv("price_tip_data.csv", header=True, index=False)

