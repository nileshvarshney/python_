from pyspark.sql import SparkSession
from dependency.logger import Logger


def start_spark(app_name = 'spark_builder'):
    spark_builder  = (
        SparkSession
        .builder
        .appName(app_name))


    # create session
    spark_sess = spark_builder.getOrCreate()
    spark_logger =  Logger(spark_sess)

    return spark_sess, spark_logger

