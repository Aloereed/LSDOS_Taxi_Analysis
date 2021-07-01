import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler
from sparktorch import SparkTorch, serialize_torch_obj
from pyspark.sql.functions import rand
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.pipeline import Pipeline, PipelineModel
from sparktorch import PysparkPipelineWrapper
import torch
import torch.nn as nn
import matplotlib.pyplot as plt


if __name__ == '__main__':
    spark = (SparkSession
                .builder
                .appName('dist_loc')
                .master("local[75]")
                .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                .enableHiveSupport()
                .getOrCreate()
                )
    
    pickup_train = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("./input/pu_train.csv")
    pickup_test = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("./input/pu_test.csv")


    vector_assembler = VectorAssembler(inputCols = ['puday', 'puhour'], outputCol = 'features')

    network = nn.Sequential(
        nn.Linear(2, 64),
        nn.ReLU(),
        nn.Linear(64, 128),
        nn.ReLU(),
        nn.Linear(128, 256),
        nn.ReLU(),
        nn.Linear(256, 1)
    )

    # Build the pytorch object
    torch_obj = serialize_torch_obj(
        model=network,
        criterion=nn.MSELoss(),
        optimizer=torch.optim.Adam,
        lr=0.001
    )


    # Demonstration of some options. Not all are required
    # Note: This uses the barrier execution mode, which is sensitive to the number of partitions
    spark_model = SparkTorch(
        inputCol='features',
        labelCol='hour_count',
        predictionCol='predictions',
        torchObj=torch_obj,
        iters=2000,
        verbose=1,
        miniBatch=256,
        earlyStopPatience=40,
        validationPct=0.2
    )

    # Create and save the Pipeline
    p = Pipeline(stages=[vector_assembler, spark_model]).fit(pickup_train)
    p.write().overwrite().save('simple_dnn')

    # Example of loading the pipeline
    loaded_pipeline = PysparkPipelineWrapper.unwrap(PipelineModel.load('simple_dnn'))

    # Run predictions and evaluation
    predictions = loaded_pipeline.transform(pickup_test).persist()
    evaluator = RegressionEvaluator(
        labelCol="hour_count", predictionCol="predictions", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
    predictions.show()
    predictions.select("pumonth", "hour_count", "predictions").toPandas().drop(0).to_csv('result.csv',header = True, index = False)

