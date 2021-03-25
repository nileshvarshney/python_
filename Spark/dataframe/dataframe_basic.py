import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, DoubleType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('Basics').getOrCreate()

# Schema
store_schema = StructType([
    StructField('STORE_ID', StringType(), False),
    StructField('STORE_LOCATION', StringType(), False),
    StructField('PRODUCT_CATEGORY', StringType(), False),
    StructField('PRODUCT_ID', IntegerType(), False),
    StructField('MRP', DoubleType(), False),
    StructField('CP', DoubleType(), False),
    StructField('DISCOUNT', DoubleType(), False),
    StructField('SP', DoubleType(), False),
    StructField('Date', DateType(), False)]
)

df = spark.read.csv('../dataset/store_data.csv', schema=store_schema, header = True)

# display random 5 rows
df.show(5)

# display dataframe schema
df.printSchema()

# display data frmae columns
print(df.columns)

# describe descriptive statistics
df.describe().show()

# selected columns
df.select(['PRODUCT_CATEGORY','SP','Date']).show(10, truncate=False)


# adding new columns
df.withColumn('PROFIT',df['SP'] - df['CP']).show(5, truncate=False)

# convert to SQL style
df.createOrReplaceTempView('store_sale')

result = spark.sql("select PRODUCT_CATEGORY, CP, SP, SP - CP as PROFIT from store_sale")
result.show(5)


# Get store wise daily profit
daily_profit_sql = """
    SELECT 
        Date,
        STORE_ID,
        ROUND((SUM(SP) - SUM(CP)),2) PROFIT
    FROM store_sale
    GROUP BY Date,STORE_ID
    ORDER BY Date,STORE_ID
"""

daily_store_profit = spark.sql(daily_profit_sql)
#daily_store_profit.show(3)
daily_store_profit.coalesce(1).write.format('com.databricks.spark.csv').save('../output/daily_store_profit.csv',header = 'true')

# Location wise daily Profit
location_wise_profit_sql = """
    SELECT 
        Date,
        STORE_LOCATION,
        ROUND((SUM(SP) - SUM(CP)),2) PROFIT
    FROM store_sale
    GROUP BY Date,STORE_LOCATION
    ORDER BY 3 DESC
"""


location_wise_profit = spark.sql(location_wise_profit_sql)
location_wise_profit.coalesce(1).write.format('com.databricks.spark.csv').save('../output/location_wise_profit.csv',header = 'true')



# Top Selling products in New York
top_selling_prod_category = \
df.filter(df.STORE_LOCATION == 'New York') \
    .select(['PRODUCT_CATEGORY','MRP','CP','SP']) \
    .groupBy(['PRODUCT_CATEGORY']) \
    .agg(F.min('MRP').alias("MIM_MPR"), \
        F.max('MRP').alias('MAX_MRP') ,\
        F.min('CP').alias('MIN_CP'), \
        F.max('CP').alias('MAX_CP'), \
        F.sum('MRP').alias('SUM_MRP'), \
        F.bround(F.sum("CP"),2).alias('SUM_CP'),\
        F.bround(F.sum('SP'),2).alias('SUM_SP'), \
        F.count('MRP').alias('COUNT_MRP')) \
    .orderBy(F.col('COUNT_MRP').desc()).limit(5)
# Write data back to output file
top_selling_prod_category.coalesce(1).write.format('com.databricks.spark.csv').save('../output/top_selling_prod_category.csv',header = 'true')


# Working with date
date_data_output = \
df.select( \
    F.dayofyear(df['Date']).alias("DAY_OF_YEAR"),
    F.month(df['Date']).alias("MONTH"),
    F.dayofweek(df['Date']).alias("DAY_OF_WEEEK"),
    F.dayofmonth(df['Date']).alias("DAY_OF_MONTH"),
    F.year(df['Date']).alias("YEAR"),
    )\
    .distinct()

# Write data back to output file
#date_data_output.coalesce(1).write.csv('../output/date_data_output.csv')
date_data_output.coalesce(1).write.format('com.databricks.spark.csv').save('../output/date_data_output.csv',header = 'true')
