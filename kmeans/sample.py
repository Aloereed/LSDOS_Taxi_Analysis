import findspark
findspark.init()

from pyspark import  SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import col

import matplotlib.pyplot as plt

spark = (SparkSession
                .builder
                .appName('kmeans')
                .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                .enableHiveSupport()
                .getOrCreate()
                )
 
# 生成查询的SQL语句，这个跟hive的查询语句一样，所以也可以加where等条件语句
hive_database = "taxi"
hive_table = "trips"
hive_read = "select * from  {}.{}".format(hive_database, hive_table)
 
# 通过SQL语句在hive中查询的数据直接是dataframe的形式
taxi = spark.sql(hive_read) # 读入数据
taxi=taxi.na.drop()
taxi = taxi.filter(taxi.pulocationid.isNotNull())
taxi = taxi.filter(taxi.total_amount.isNotNull())
taxi = taxi.withColumn("pulocationid",col("pulocationid").cast("Integer"))
taxi = taxi.filter(taxi.pulocationid < 264)
pu_money = taxi.groupby('pulocationid').agg(F.mean('total_amount').alias('avg_money'))
pu_money.sort(pu_money.avg_money.desc())
pu_money.toPandas().to_csv('/home/hadoop/pj/kmeans/pu_money.csv',header = True, index = False)

vecAssembler = VectorAssembler(inputCols=["avg_money"], outputCol="features")
pu_money_km = vecAssembler.transform(pu_money).select('pulocationid','features')

# pu_money_km = pu_money.withColumn('features',pu_money.avg_money)
# pu_money_km = pu_money_km.select('pulocationid','features')
pu_money_km.printSchema()
evaluator = ClusteringEvaluator(predictionCol='prediction', \
                                metricName='silhouette', distanceMeasure='squaredEuclidean')

# silhouette_score = []
# for k in range(2, 10):
#     kmeans = KMeans(k=k, seed=1)
#     km_model = kmeans.fit(pu_money_km)
#     predictions = km_model.transform(pu_money_km)
    
#     silhouette_score.append(evaluator.evaluate(predictions))

# fig, ax = plt.subplots(1,1, figsize=(8,6))
# ax.plot(range(2,10), silhouette_score)
# ax.set_xlabel('k')
# ax.set_ylabel('cost')
# plt.savefig("/home/hadoop/pj/kmeans/k_cost.jpg")

kmeans = KMeans(k=6, seed=1)
km_model = kmeans.fit(pu_money_km)
centers = km_model.clusterCenters()
print(centers)

transformed = km_model.transform(pu_money_km).select('pulocationid', 'prediction')
transformed.sort(transformed.prediction.desc()).toPandas().to_csv('/home/hadoop/pj/kmeans/pu_means_kmeans.csv',header = True, index = False)


