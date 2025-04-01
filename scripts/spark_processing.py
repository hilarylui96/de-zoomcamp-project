
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
from pyspark.sql.functions import col

import sys

def coords_to_wkt(geom_type, coords):
  if geom_type == "Polygon":
      if coords and isinstance(coords, list):
          rings = []
          for ring in coords:
              ring_str = ", ".join([f"{lon} {lat}" for lon, lat in ring])
              rings.append(f"({ring_str})")
          return f"POLYGON({', '.join(rings)})"
  return None

def main():
  execution_date = sys.argv[1]
  gcs_path = sys.argv[2]
  project_id = sys.argv[3]
  stag_dataset_id = sys.argv[4]
  prod_dataset_id = sys.argv[5]
  temp_bucket = sys.argv[6]

  spark = SparkSession.builder \
    .appName("DataProcessing") \
    .getOrCreate()

  geometry_schema = T.StructType(
      [
        T.StructField("coordinates", T.ArrayType(  
            T.ArrayType(                         
                T.ArrayType(T.DoubleType())        
            )
        ), True),
        T.StructField("type", T.StringType(), True),
      ])
  properties_schema = T.StructType([
      T.StructField("affectedZones", T.ArrayType(T.StringType()), True),
      T.StructField("areaDesc", T.StringType(), True),
      T.StructField("category", T.StringType(), True),
      T.StructField("certainty", T.StringType(), True),
      T.StructField("description", T.StringType(), True),
      T.StructField("effective", T.StringType(), True),
      T.StructField("ends", T.StringType(), True),
      T.StructField("event", T.StringType(), True),
      T.StructField("expires", T.StringType(), True),
      T.StructField("headline", T.StringType(), True),
      T.StructField("id", T.StringType(), True),
      T.StructField("instruction", T.StringType(), True),
      T.StructField("messageType", T.StringType(), True),
      T.StructField("onset", T.StringType(), True),
      T.StructField("replacedAt", T.StringType(), True),
      T.StructField("replacedBy", T.StringType(), True),
      T.StructField("response", T.StringType(), True),
      T.StructField("sender", T.StringType(), True),
      T.StructField("senderName", T.StringType(), True),
      T.StructField("sent", T.StringType(), True),
      T.StructField("severity", T.StringType(), True),
      T.StructField("status", T.StringType(), True),
      T.StructField("urgency", T.StringType(), True),
    ])
  
  data_schema = T.StructType([
      T.StructField("geometry", geometry_schema, True),
      T.StructField("id", T.StringType(), True),
      T.StructField("properties", properties_schema, True),
      T.StructField("type", T.StringType(), True),
  ])

  wkt_udf = F.udf(coords_to_wkt, T.StringType())

  df = spark.read \
    .option("multiline", "true") \
    .schema(data_schema) \
    .json(f"{gcs_path}/raw_data/{execution_date}.json")
  
  df = df.select(
    wkt_udf("geometry.type", "geometry.coordinates").alias("geometry_wkt"),
    F.explode(col("properties.affectedZones")).alias("affectedZone"),
    col("properties.areaDesc").alias("areaDesc"),
    col("properties.category").alias("category"),
    col("properties.certainty").alias("certainty"),
    col("properties.description").alias("description"),
    F.to_timestamp(col("properties.effective")).alias("effective"),
    F.to_timestamp(col("properties.ends")).alias("ends"),
    col("properties.event").alias("event"),
    F.to_timestamp(col("properties.expires")).alias("expires"),
    col("properties.headline").alias("headline"),
    col("properties.id").alias("alert_id"),
    col("properties.instruction").alias("instruction"),
    col("properties.messageType").alias("messageType"),
    col("properties.onset").alias("onset"),
    F.to_timestamp(col("properties.replacedAt")).alias("replacedAt"),
    col("properties.replacedBy").alias("replacedBy"),
    col("properties.response").alias("response"),
    col("properties.sender").alias("sender"),
    col("properties.senderName").alias("senderName"),
    F.to_timestamp(col("properties.sent")).alias("sent"),
    col("properties.severity").alias("severity"),
    col("properties.status").alias("status"),
    col("properties.urgency").alias("urgency")
  )
   
  # add unique id
  df = df.select(
    F.md5(F.concat(F.coalesce(col('alert_id'),F.lit("")),F.coalesce(col('affectedZone'),F.lit("")))).alias("id"),
    "*"
  )
  
  # Create Staging Table
  df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", temp_bucket) \
    .mode("overwrite") \
    .option("project", project_id) \
    .option("dataset", stag_dataset_id) \
    .option("table", "alerts_{execution_date}") \
    .save()
  
  # Get New Rows
  existing = spark.read \
    .format("bigquery") \
    .option("project", project_id) \
    .option("dataset", prod_dataset_id) \
    .option("table", "weather_alerts") \
    .load() 
  
  new_rows = df.alias("new").join(
     existing.select("id").alias("existing"),
     on=col("new.id") == col("existing.id"),
     how='left_anti'
  )

  # Append to Prod
  new_rows.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", temp_bucket) \
    .mode("append") \
    .option("project", project_id) \
    .option("dataset", prod_dataset_id) \
    .option("table", "weather_alerts") \
    .save()

  spark.stop()

if __name__ == "__main__":
  main()
