import findspark
findspark.init()

from pyspark import  SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, FloatType

from scipy.stats import chi2_contingency

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

#%% Functions

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
    splits = (-5, 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70)
    return cutme_prototype(value, splits)

@udf
def label_total(rate):
    splits = (-5, 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70)
    tags = ["<{}".format(splits[0])]
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
    return tags[rate]


#%%

df2 = df.select(label_total(cut_total('total_amount')).alias("total"), label_tip(cut_tip('tip_amount')).alias("tip"))
ct = df2.sort(df2.total.asc()).crosstab("total", "tip")
ct.sort(ct.total_tip.asc()).show()

#%%

arr = ct.toPandas().values
res = chi2_contingency(arr[:,1:])

print(">> p-value is {}, therefore we reject H_0:Independent. Which means that paytype and distance are correlated.".format(res[1]))
print(res)
