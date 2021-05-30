import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import hour,desc


spark = SparkSession.builder.enableHiveSupport().getOrCreate()

df_data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("./input/Taxidata_20180501_20180526.csv")

# df_data.printSchema()

df_data_cast0 = df_data.withColumn("tpepPickupDateTime",col("tpepPickupDateTime").cast(TimestampType()))
df_data_cast1 = df_data_cast0.withColumn("puLocationId",col("puLocationId").cast("Integer"))
df_data_cast = df_data_cast1.withColumn("doLocationId",col("doLocationId").cast("Integer"))
# df_data_cast.printSchema()
# df_data_cast.show()

df_data_selected =  df_data_cast.select(df_data_cast.tpepPickupDateTime,df_data_cast.puLocationId,df_data_cast.doLocationId)

# df_data_selected.printSchema()
# df_data_selected.show()

# df_data_hour = df_data_selected.withColumn("tpepPickupDateTime",F.date_trunc('hour',F.to_timestamp("tpepPickupDateTime","yyyy-MM-dd HH:mm:ss 'UTC'")))
df_data_hour = df_data_selected.withColumn("tpepPickupDateTime",hour(col("tpepPickupDateTime")))

df_data_hour.printSchema()
# df_data_hour.show()

df_count_pudo = df_data_hour.groupby(df_data_hour.tpepPickupDateTime,df_data_hour.puLocationId,df_data_hour.doLocationId).count()

df_count_pudo.show()

df_count_order = df_count_pudo.orderBy("tpepPickupDateTime","count",ascending=False)

df_count_order.show()

df_count_order.write.csv('./output')