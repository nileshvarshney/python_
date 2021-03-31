# Question to Answer?
# 1. What were all the differrent types of fire calls in 2018?
# 2. What month within the year 2018 saw the higest number of fire call?
# 3. Which neighborhood genrated the most of fire call in 2018?
# 4. Which week of in the year 2018 had most fire call?


import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#to_timestamp, to_date, col, year, month, weekofyear

# root
#  |-- CallNumber: integer (nullable = true)
#  |-- UnitID: string (nullable = true)
#  |-- IncidentNumber: integer (nullable = true)
#  |-- CallType: string (nullable = true)
#  |-- CallDate: string (nullable = true)
#  |-- WatchDate: string (nullable = true)
#  |-- CallFinalDisposition: string (nullable = true)
#  |-- AvailableDtTm: string (nullable = true)
#  |-- Address: string (nullable = true)
#  |-- City: string (nullable = true)
#  |-- Zipcode: integer (nullable = true)
#  |-- Battalion: string (nullable = true)
#  |-- StationArea: string (nullable = true)
#  |-- Box: string (nullable = true)
#  |-- OriginalPriority: string (nullable = true)
#  |-- Priority: string (nullable = true)
#  |-- FinalPriority: integer (nullable = true)
#  |-- ALSUnit: boolean (nullable = true)
#  |-- CallTypeGroup: string (nullable = true)
#  |-- NumAlarms: integer (nullable = true)
#  |-- UnitType: string (nullable = true)
#  |-- UnitSequenceInCallDispatch: integer (nullable = true)
#  |-- FirePreventionDistrict: string (nullable = true)
#  |-- SupervisorDistrict: string (nullable = true)
#  |-- Neighborhood: string (nullable = true)
#  |-- Location: string (nullable = true)
#  |-- RowID: string (nullable = true)
#  |-- Delay: double (nullable = true)
#  |-- IncidentDate: date (nullable = true)
#  |-- OnWatchDate: date (nullable = true)
#  |-- OnAvailableDtTm: timestamp (nullable = true)

def open_spark_session(app_name):
    ses = SparkSession\
        .builder\
        .appName(app_name)\
        .getOrCreate()
    return ses


def read_input_csv(ses, input_file):
    df = ses.read.csv(input_file, inferSchema=True, header=True)
    return df

def convert_df_date_columns(ses, df):
    temp_df = df\
        .withColumn("IncidentDate", to_date(col("CallDate"),"MM/dd/yyyy"))\
        .withColumn("OnWatchDate", to_date(col("WatchDate"),"MM/dd/yyyy"))\
        .withColumn("OnAvailableDtTm", to_timestamp(col("CallDate"),"MM/dd/yyyy hh:mm:ss a"))
    return temp_df

def call_type_by_year(df, i_year):
    temp_df = df\
        .select("CallType")\
        .where(year(col("IncidentDate")) == i_year)\
        .groupBy("CallType")\
        .count()\
        .orderBy("count", ascending=False)
    return temp_df

def highest_call_by_month(df, i_year):
    temp_df = df\
        .withColumn("IncidentMonth",month(col("IncidentDate")))\
        .select("IncidentMonth")\
        .where(year(col("IncidentDate")) == i_year)\
        .groupBy("IncidentMonth")\
        .count()\
        .orderBy("count", ascending=False)
    return temp_df


def calls_by_neighborhood(df, i_year):
    temp_df = df\
        .select("Neighborhood")\
        .where(year(col("IncidentDate")) == i_year)\
        .groupBy("Neighborhood")\
        .count()\
        .orderBy("count", ascending=False)
    return temp_df


def highest_call_by_week(df, i_year):
    temp_df = df\
        .withColumn("IncidentWeek",weekofyear(col("IncidentDate")))\
        .select("IncidentWeek")\
        .where(year(col("IncidentDate")) == i_year)\
        .groupBy("IncidentWeek")\
        .count()\
        .orderBy("count", ascending=False)
    return temp_df



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Usage: sf_fire_call.py <input file>', file=sys.stderr)
        sys.exit(-1)

    data = sys.argv[1]
    spark = open_spark_session('sf_fire_call')
    fire_df = read_input_csv(spark, data)
    fire_df = convert_df_date_columns(spark,fire_df )
    # fire_df.printSchema()



    # What were all the differrent types of fire calls in 2018?
    call_type_by_year_df = call_type_by_year(fire_df, 2018)

    # What month within the year 2018 saw the higest number of fire call?
    highest_call_by_month_df = highest_call_by_month(fire_df, 2018)

    # Which neighborhood genrated the most of fire call in 2018?
    calls_by_neighborhood_df = calls_by_neighborhood(fire_df, 2018)

    # Which week of in the year 2018 had most fire call?
    highest_call_by_week_df  = highest_call_by_week(fire_df, 2018)


    # save output in parquet format
    parquet_table = "calls_by_week"
    #highest_call_by_week_df.write.format("parquet").saveAsTable(parquet_table)

    # distinct call type
    count_distinct_call_type = fire_df\
        .select("CallType")\
        .where(col("CallType").isNotNull())\
        .agg(countDistinct("CallType").alias("DistinctCallType"))\
        .show()


    fire_df\
        .select("CallType")\
        .where(col("CallType").isNotNull())\
        .distinct()\
        .show(10, truncate=False)

    # column Rename
    fire_df.withColumnRenamed("Delay", "ResponseDelayinMins")\
        .select("ResponseDelayinMins")\
        .where(col("ResponseDelayinMins") > 5)\
        .orderBy("ResponseDelayinMins", ascending=False)\
        .show(5, truncate= False)

    # Min, max, Avg
    fire_df.withColumnRenamed("Delay", "ResponseDelayinMins")\
        .select(min("ResponseDelayinMins"),\
            max("ResponseDelayinMins"),
            avg("ResponseDelayinMins"))\
            .show( truncate=False)


