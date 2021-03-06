from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName('raw_to_processed')\
    .config("hive.metastore.connect.retries", 5)\
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")\
    .enableHiveSupport()\
    .getOrCreate()

df = spark.read.format('json').load('s3://s3-belisco-production-data-lake-raw/atomic_events')
df.write.format('parquet').save('s3://s3-belisco-production-data-lake-processed/atomic_events')
