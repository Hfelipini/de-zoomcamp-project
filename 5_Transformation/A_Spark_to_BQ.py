#!/usr/bin/env python

"""BigQuery I/O PySpark example."""

from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
bucket = "dtc_data_lake_de-zoomcamp-project-cluster-hfelipini"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
python_test = spark.read.format('bigquery') \
  .option('table', 'de-zoomcamp-project-hfelipini:de_project_dataset.python_test_partitioned') \
  .load()
python_test.createOrReplaceTempView('python_test')

# Perform calculation to check rate of variation in each ticker.
minute_rate = spark.sql(
    """
      SELECT
        DISTINCT(A.ID), A.DateTime, A.Ticker, A.Close, A.Timeframe, A.Dayframe, B.Close as ClosePrevious, CAST(A.Close AS DECIMAL) / B.Close - 1 as Rate
      FROM python_test A,
           python_test B 
      WHERE 
        A.Ticker = B.Ticker
      AND 
        A.Timeframe - 1 = B.Timeframe
      AND  
        A.Dayframe = (
          SELECT MAX(Dayframe) 
          FROM python_test
        )
      AND 
        A.Timeframe >= (
          SELECT MAX(Timeframe) 
          FROM python_test 
          WHERE Dayframe = (
            SELECT MAX(Dayframe) 
            FROM python_test
          )
        )
      ORDER BY Rate DESC
    """)
minute_rate.show()
minute_rate.printSchema()

# Saving the data to BigQuery
minute_rate.write.format('bigquery').option('table', 'de-zoomcamp-project-hfelipini.de_project_dataset.minute_rate').mode("overwrite").save()