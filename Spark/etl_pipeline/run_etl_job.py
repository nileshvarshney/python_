import pyspark
from pyspark.sql import SparkSession
from dependency.spark_ses import start_spark
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, StructField, StructType
from pyspark.sql import functions as F



def main():
    spark_sess, log = start_spark(
        app_name = 'simple_etl_job'
    )

    log.warn('*******************************************************************')
    log.warn('***************** simple_etl_job is up and running ****************')
    log.warn('*******************************************************************')

    raw_df = extract_data(spark_sess)
    create_temp_tableOrView(raw_df, 'store_sale')
    transformed_df = transform_data(spark_sess, 'store_sale')
    load_data(transformed_df)


def extract_data(sess):
    """
        Load data from CSV format
        :param sess: spark session object
        :return: Spark dataframe
    """

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
    df = sess.read.csv('data/store_data.csv', schema=store_schema, header = True)
    return df

def create_temp_tableOrView(df, view_name):
    """
        Create or Replace Temp View
        :param sess: spark data frame
        :param view_name : View Name
        :return: None
    """

    df.createOrReplaceTempView(view_name)
    return None

def transform_data(sess, view_name):
    location_wise_profit_sql = """
            SELECT 
                Date,
                STORE_LOCATION,
                ROUND((SUM(SP) - SUM(CP)),2) PROFIT 
            FROM """ + view_name + """
            GROUP BY Date,STORE_LOCATION
            ORDER BY 3 DESC
        """

    df = sess.sql(location_wise_profit_sql)  
    return df

def load_data(df):
    df.coalesce(1).write.format('com.databricks.spark.csv').save('output/data_data_output.csv',header = 'true')
    return None


if __name__ == '__main__':
    main()