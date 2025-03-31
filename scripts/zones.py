from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import col
import sys

def main():

  gcs_path = sys.argv[1]
  project_id = sys.argv[2]
  dataset_id = sys.argv[3]
  temp_bucket = sys.argv[4]

  properties_schema = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("state", T.StringType(), True)
  ])
  
  data_schema = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("properties", properties_schema, True)
  ])
  spark = SparkSession.builder \
    .appName("ProcessZone") \
    .getOrCreate()
  
  zone = spark.read \
    .option("multiline", "true") \
    .schema(data_schema) \
    .json(f"{gcs_path}/raw_data/zones.json")
  
  zone = zone \
    .withColumn('name', col('properties.name')) \
    .withColumn('state', col('properties.state')) \
    .drop('properties')
  
  zone.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", temp_bucket) \
    .mode("overwrite") \
    .option("table", f'{project_id}.{dataset_id}.zone_lookup') \
    .save()
  
  spark.stop()
  
if __name__ == '__main__':
  main()
  