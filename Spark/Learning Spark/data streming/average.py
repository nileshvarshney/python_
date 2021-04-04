import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    host, port = ('127.0.0.1', 65432)

    spark = SparkSession\
        .builder\
        .appName('Average')\
        .getOrCreate()


    lines = spark\
        .readStream\
        .format("socket")\
        .option("host", host)\
        .option("port", port)\
        .load()

    CSVData = lines.select(
        split(lines.value,",").getItem(0).alias("eventTime").cast('timestamp'),\
        split(lines.value,",").getItem(1).alias("reading").cast('int'),\
        split(lines.value, ",").getItem(2).alias("deviceID"),\
    )

    readingByDeviceTime = CSVData.groupBy("deviceID",\
         window('eventTime', windowDuration='30 seconds',slideDuration='30 seconds'))\
        .avg("reading")\
        .sort("deviceID")

    query = readingByDeviceTime.writeStream.outputMode('complete').format('console').start()

    query.awaitTermination()