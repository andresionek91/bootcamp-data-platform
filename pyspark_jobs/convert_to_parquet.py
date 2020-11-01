from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('raw_to_processed').getOrCreate()

df = spark.read.format('json').load('s3://s3-belisco-production-data-lake-raw/atomic_events')
df.write.format('parquet').save('s3://s3-belisco-production-data-lake-cooked/atomic_events')

