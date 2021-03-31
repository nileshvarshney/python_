from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount.py <filename>", file=sys.stderr)
        sys.exit(-1)

    mnm_file = sys.argv[1]
    spark = SparkSession\
        .builder.appName("MNMCount").getOrCreate()


mnm_df = spark.read.csv(mnm_file, header=True, inferSchema=True)
mnm_df.show(n = 5, truncate=False)

# aggregate counts of all colors and group by state and order by descending
count_mnm_df = mnm_df.select("State", "Color", "Count")\
    .groupBy("State", "Color")\
    .sum("Count").withColumnRenamed('sum(Count)', 'sum_count')\
    .orderBy("sum_count", ascending=False)


count_mnm_df.show(10, truncate=False)

# find aggregate count for california only
ca_count_mnm_df = mnm_df.select("*")\
    .filter(mnm_df.State =="CA")\
    .groupBy("State", "Color")\
    .sum("Count")\
    .withColumnRenamed('sum(Count)', 'sum_count')\
    .orderBy("sum_count", ascending=False)


ca_count_mnm_df.show(10, truncate=False)

