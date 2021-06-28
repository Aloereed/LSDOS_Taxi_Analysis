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


#%%

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
def cut_dist(value):
    splits = tuple(i for i in range(0, 100, 10))
    return cutme_prototype(value, splits)

@udf
def label_dist(rate):
    splits = tuple(i for i in range(0, 100, 10))
    tags = ["<{}".format(splits[0])]
    for i in range(len(splits) - 1):
        tags.append("{}~{}".format(splits[i], splits[i+1]))
    tags.append(">{}".format(splits[-1]))
    return tags[rate]


#%%

ct = df.select("payment_type", label_dist(cut_dist("trip_distance")).alias("dist")).crosstab("payment_type", "dist")

#%%

arr = ct.toPandas().values
res = chi2_contingency(arr[:,1:])

# print(res)
print(">> p-value is {}, therefore we reject H_0:Independent. Which means that paytype and distance are correlated.".format(res[1]))
