import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    host, port = ('127.0.0.1', 65431)


    spark = SparkSession\
        .builder\
        .appName('simple')\
        .getOrCreate()


    lines = spark\
        .readStream\
        .format("socket")\
        .option("host", host)\
        .option("port", port)\
        .load()

    CSVData = lines.select(\
        split(lines.value,",").getItem(0).alias("eventTime").cast("timestamp"),
        split(lines.value,",").getItem(1).alias("BloodGlucose").cast("int"),
        split(lines.value,",").getItem(2).alias("DeviceID")
    )

    selectAndFilter = CSVData.select("eventTime","BloodGlucose")\
        .where("BloodGlucose > 0")

    query = selectAndFilter\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .start()

    query.awaitTermination()