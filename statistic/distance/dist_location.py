import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
    taxi = taxi.filter('trip_distance>0.5 and trip_distance<100 ')
    # taxi.show(10)
    taxi = taxi.na.drop()
    # taxi = spark.read.csv('Taxidata_20190501_20190602.csv', header = True)
    # taxi.createOrReplaceTempView('taxi')
    # spark.hour_sql('''SELECT AVG(tripDistance) AS avg_dist, stdev(tripDistance) AS sd_dist FROM taxi GROUP BY puLocationID''').show()

    # 路程长度和地域的关系(以某一个id为起终点的距离均值和标准差）
    pu_dist = taxi.groupby('PULocationID').agg(F.mean('trip_distance').alias('avg_dist'),\
        F.stddev('trip_distance').alias('sd_dist'))
    pu_dist.sort(pu_dist.avg_dist.desc()).filter('PULocationID is not NULL')\
        .toPandas().to_csv('pu_dist_location.csv',header = True, index = False)
    do_dist = taxi.groupby('DOLocationID').agg(F.mean('trip_distance').alias('avg_dist'),\
        F.stddev('trip_distance').alias('sd_dist'))
    do_dist.sort(do_dist.avg_dist.desc()).filter('DOLocationID is not NULL').toPandas()\
        .to_csv('do_dist_location.csv',header = True, index = False)