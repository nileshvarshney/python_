from pyspark.spark import start_spark


def main():
    spark, log, config = start_spark(
        app_name = 'simple_etl_job',
        files = ['configs/run_config.json']
    )

if __name__ == '__main__':
    main()