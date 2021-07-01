import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark import SparkConf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
if __name__ == '__main__':
    spark = (SparkSession
                .builder
                .appName('dist_loc')
                .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                .enableHiveSupport()
                .getOrCreate()
                )

    pickup_train = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("./input/pu_train.csv")
    pickup_test = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("./input/pu_test.csv")


    vectorAssembler = VectorAssembler(inputCols = ['puday', 'puhour'], outputCol = 'features')
    train_df = vectorAssembler.transform(pickup_train)
    vtrain_df = train_df.select(['features', 'hour_count'])
    test_df = vectorAssembler.transform(pickup_test)
    vtest_df = test_df.select(['features', 'hour_count'])

    lr = RandomForestRegressor(featuresCol = 'features', labelCol='hour_count')
    lr_model = lr.fit(vtrain_df)


    predictions = lr_model.transform(vtest_df)
    evaluator = RegressionEvaluator(labelCol="hour_count", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    predictions.show()