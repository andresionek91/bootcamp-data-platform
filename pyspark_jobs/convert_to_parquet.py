from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName('raw_to_processed')\
    .config("hive.metastore.connect.retries", 5)\
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")\
    .enableHiveSupport()\
    .getOrCreate()

df = spark.read.format('json').load('s3://s3-belisco-production-data-lake-raw/atomic_events')
df.write.format('parquet').save('s3://s3-belisco-production-data-lake-processed/atomic_events')




# from pyspark.sql import SparkSession
# import json
# from pyspark.sql.functions import udf
# from pyspark.sql import functions as F
# from pyspark.sql.types import *
#
#
# spark = SparkSession.builder\
#     .appName('raw_to_processed')\
#     .config("hive.metastore.connect.retries", 5)\
#     .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")\
#     .enableHiveSupport()\
#     .getOrCreate()
#
#
# partitions = ('year', 'month', 'day', 'hour')
# json_schema = spark.table("glue_belisco_production_data_lake_raw.atomic_events").schema
# json_schema = StructType([field for field in json_schema if field.name not in partitions])
#
#
# def lowercase_keys(obj):
#     if isinstance(obj, dict):
#         obj = {key.lower(): value for key, value in obj.items()}
#         for key, value in obj.items():
#             if isinstance(value, list):
#                 for idx, item in enumerate(value):
#                     value[idx] = lowercase_keys(item)
#             obj[key] = lowercase_keys(value)
#     return obj
#
#
# def parse_json_and_lowercase_keys(json_str):
#     parsed_json = json.loads(json_str)
#     return lowercase_keys(parsed_json)
#
#
# udf_parse_json = udf(lambda json_str: parse_json_and_lowercase_keys(json_str), json_schema)
#
# df = spark.read.text("s3://s3-belisco-production-data-lake-raw/atomic_events")
# df = df.select(udf_parse_json(F.col("value")).alias("json_data"), *partitions)
# df = df.select("json_data.*", *partitions)
# df.write.format('parquet').save('s3://s3-belisco-production-data-lake-processed/atomic_events')
#

