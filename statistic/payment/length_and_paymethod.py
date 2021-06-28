import findspark
findspark.init()

from pyspark import  SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (SparkSession
         .builder
         .appName('example-pyspark-read-and-write-from-hive')
         .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
         .enableHiveSupport()
         .getOrCreate()
         )

# df = spark.read.csv("sampled_data.csv", header=True)
df = spark.sql("select * from taxi.trips where isNotNull(vendorid)")


res = df.groupby('payment_type').agg(F.mean(df.trip_distance).alias("dist_avg"), F.variance(df.trip_distance).alias("dist_var"))
res.sort(df.payment_type.asc()).toPandas().to_csv('paytype_dist.csv',header = True, index = False)