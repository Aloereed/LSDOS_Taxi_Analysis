import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark import SparkConf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
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
    taxi = taxi.withColumn('PULocationID',taxi['PULocationID'].cast(IntegerType()))\
        .withColumn('DOLocationID',taxi['DOLocationID'].cast(IntegerType()))
    taxi = taxi.withColumn('trip_distance',taxi['trip_distance'].cast(FloatType()))
    taxi = taxi.withColumn('time',F.abs(F.unix_timestamp('tpep_pickup_datetime')-F.unix_timestamp('tpep_dropoff_datetime')))

    taxi.createOrReplaceTempView('taxi')

    train_sql = '''
    select time, trip_distance as distance, fare_amount as fare
    from taxi
    where PULocationID = 21 and time > 0 and trip_distance > 0 and fare_amount > 0
    '''
    fare_train = spark.sql(train_sql)

    test_sql = '''
    select time, trip_distance as distance, fare_amount as fare
    from taxi
    where PULocationID = 20 and time > 0 and trip_distance > 0 and fare_amount > 0
    '''
    fare_test = spark.sql(test_sql)

    # print(fare_train.stat.corr('time', 'fare'))
    # print(fare_train.stat.corr('distance', 'fare'))


    vectorAssembler = VectorAssembler(inputCols = ['time', 'distance'], outputCol = 'features')
    train_df = vectorAssembler.transform(fare_train)
    vtrain_df = train_df.select(['features', 'fare'])
    test_df = vectorAssembler.transform(fare_test)
    vtest_df = test_df.select(['features', 'fare'])

    # lr = LinearRegression(featuresCol = 'features', labelCol='fare', maxIter=20, regParam=0.3, elasticNetParam=0.8)
    lr = RandomForestRegressor(featuresCol = 'features', labelCol='fare')
    lr_model = lr.fit(vtrain_df)
    # print("Coefficients: " + str(lr_model.coefficients))
    # print("Intercept: " + str(lr_model.intercept))

    # trainingSummary = lr_model.summary
    # print("r2: %f" % trainingSummary.r2)

    # test_result = evaluator.evaluate(vtest_df)
    predictions = lr_model.transform(vtest_df)
    evaluator = RegressionEvaluator(labelCol="fare", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
    predictions.select("prediction","fare").show()