import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, DoubleType

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
daily_store_profit.show(3)


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
location_wise_profit.show(3)

