import findspark
findspark.init()

from pyspark import  SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import hour,desc,month

spark = (SparkSession
                .builder
                .appName('example-pyspark-read-and-write-from-hive')
                .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                .enableHiveSupport()
                .getOrCreate()
                )
 
# 生成查询的SQL语句，这个跟hive的查询语句一样，所以也可以加where等条件语句
hive_database = "taxi"
hive_table = "trips"
hive_read = "select * from  {}.{}".format(hive_database, hive_table)
 
# # 通过SQL语句在hive中查询的数据直接是dataframe的形式
# read_df = spark.sql(hive_read)
# read_df.printSchema()


# 乘车路径模式分析
# taxi = spark.sql(hive_read) # 读入数据
# taxi = taxi.filter(taxi.pulocationid.isNotNull())
# taxi = taxi.filter(taxi.tpep_pickup_datetime.isNotNull())
# taxi = taxi.filter(taxi.dolocationid.isNotNull())

# df_data_cast = taxi.withColumn("tpep_pickup_datetime",col("tpep_pickup_datetime").cast(TimestampType()))
# df_data_cast = df_data_cast.withColumn("pulocationid",col("pulocationid").cast("Integer"))
# df_data_cast = df_data_cast.withColumn("dolocationid",col("dolocationid").cast("Integer"))

# df_data_filter = df_data_cast.filter(df_data_cast.pulocationid < 264)
# df_data_filter = df_data_cast.filter(df_data_cast.dolocationid < 264)

# df_data_filter.printSchema()

# df_data_selected =  df_data_filter.select(df_data_filter.tpep_pickup_datetime,df_data_filter.pulocationid,df_data_filter.dolocationid)
# df_data_hour = df_data_selected.withColumn("tpep_pickup_datetime",month(col("tpep_pickup_datetime")))

# df_count_pudo = df_data_hour.groupby(df_data_hour.tpep_pickup_datetime,df_data_hour.pulocationid,df_data_hour.dolocationid).count()

# df_count_pudo.show()

# df_count_order = df_count_pudo.orderBy("tpep_pickup_datetime","count",ascending=False)

# df_count_order.show()

# df_count_order.write.csv('./output')

# 乘车热点：出发地
# taxi = spark.sql(hive_read) # 读入数据
# taxi = taxi.filter(taxi.pulocationid.isNotNull())
# taxi = taxi.filter(taxi.tpep_pickup_datetime.isNotNull())
# taxi = taxi.filter(taxi.dolocationid.isNotNull())

# df_data_cast = taxi.withColumn("tpep_pickup_datetime",col("tpep_pickup_datetime").cast(TimestampType()))
# df_data_cast = df_data_cast.withColumn("pulocationid",col("pulocationid").cast("Integer"))

# df_data_filter = df_data_cast.filter(df_data_cast.pulocationid < 264)

# df_data_filter.printSchema()

# df_data_selected =  df_data_filter.select(df_data_filter.tpep_pickup_datetime,df_data_filter.pulocationid)
# df_data_hour = df_data_selected.withColumn("tpep_pickup_datetime",hour(col("tpep_pickup_datetime")))

# df_count_pudo = df_data_hour.groupby(df_data_hour.tpep_pickup_datetime,df_data_hour.pulocationid).count()

# df_count_pudo.show()

# df_count_order = df_count_pudo.orderBy("tpep_pickup_datetime","count",ascending=False)

# df_count_order.show()

# df_count_order.write.csv('./output')

# 乘车热点，到达地
taxi = spark.sql(hive_read) # 读入数据
taxi = taxi.filter(taxi.pulocationid.isNotNull())
taxi = taxi.filter(taxi.tpep_pickup_datetime.isNotNull())
taxi = taxi.filter(taxi.dolocationid.isNotNull())

taxi.printSchema()

df_data_cast = taxi.withColumn("tpep_dropoff_datetime",col("tpep_dropoff_datetime").cast(TimestampType()))
df_data_cast = df_data_cast.withColumn("dolocationid",col("dolocationid").cast("Integer"))

df_data_filter = df_data_cast.filter(df_data_cast.dolocationid < 264)

df_data_filter.printSchema()

df_data_selected =  df_data_filter.select(df_data_filter.tpep_dropoff_datetime,df_data_filter.dolocationid)
df_data_hour = df_data_selected.withColumn("tpep_dropoff_datetime",month(col("tpep_dropoff_datetime")))

df_count_pudo = df_data_hour.groupby(df_data_hour.tpep_dropoff_datetime,df_data_hour.dolocationid).count()

df_count_pudo.show()

df_count_order = df_count_pudo.orderBy("tpep_dropoff_datetime","count",ascending=False)

df_count_order.show()

df_count_order.write.csv('./output')