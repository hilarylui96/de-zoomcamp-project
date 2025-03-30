
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
from pyspark.sql.functions import col

def coords_to_wkt(geom_type, coords):
  if geom_type == "Polygon":
      # Flatten into "x y, x y, ..."
      if coords and isinstance(coords, list):
          rings = []
          for ring in coords:
              ring_str = ", ".join([f"{lon} {lat}" for lon, lat in ring])
              rings.append(f"({ring_str})")
          return f"POLYGON({', '.join(rings)})"
  return None

def main():

  ingestion_date = datetime.strftime(datetime.today(), '%Y-%m-%d')
  file_name = f"{datetime.today().strftime('%Y-%m-%d')}.json"
  file_name_parquet = f"{datetime.today().strftime('%Y-%m-%d')}.parquet"

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
      # T.StructField("@id", T.StringType(), True),
      # T.StructField("@type", T.StringType(), True),
      T.StructField("affectedZones", T.ArrayType(T.StringType()), True),
      T.StructField("areaDesc", T.StringType(), True),
      T.StructField("category", T.StringType(), True),
      T.StructField("certainty", T.StringType(), True),
      T.StructField("description", T.StringType(), True),
      T.StructField("effective", T.StringType(), True),
      T.StructField("ends", T.StringType(), True),
      T.StructField("event", T.StringType(), True),
      T.StructField("expires", T.StringType(), True),
      # T.StructField("geocode", T.StructType([
      #     T.StructField("SAME", T.ArrayType(T.StringType()), True),
      #     T.StructField("UGC", T.ArrayType(T.StringType()), True),
      # ]), True),
      T.StructField("headline", T.StringType(), True),
      T.StructField("id", T.StringType(), True),
      T.StructField("instruction", T.StringType(), True),
      T.StructField("messageType", T.StringType(), True),
      T.StructField("onset", T.StringType(), True),
      # T.StructField("parameters", T.StructType([
      #     T.StructField("AWIPSidentifier", T.ArrayType(T.StringType()), True),
      #     T.StructField("BLOCKCHANNEL", T.ArrayType(T.StringType()), True),
      #     T.StructField("CMAMlongtext", T.ArrayType(T.StringType()), True),
      #     T.StructField("CMAMtext", T.ArrayType(T.StringType()), True),
      #     T.StructField("EAS-ORG", T.ArrayType(T.StringType()), True),
      #     T.StructField("NWSheadline", T.ArrayType(T.StringType()), True),
      #     T.StructField("VTEC", T.ArrayType(T.StringType()), True),
      #     T.StructField("WEAHandling", T.ArrayType(T.StringType()), True),
      #     T.StructField("WMOidentifier", T.ArrayType(T.StringType()), True),
      #     T.StructField("eventEndingTime", T.ArrayType(T.StringType()), True),
      #     T.StructField("eventMotionDescription", T.ArrayType(T.StringType()), True),
      #     T.StructField("expiredReferences", T.ArrayType(T.StringType()), True),
      #     T.StructField("flashFloodDetection", T.ArrayType(T.StringType()), True),
      #     T.StructField("hailThreat", T.ArrayType(T.StringType()), True),
      #     T.StructField("maxHailSize", T.ArrayType(T.StringType()), True),
      #     T.StructField("maxWindGust", T.ArrayType(T.StringType()), True),
      #     T.StructField("thunderstormDamageThreat", T.ArrayType(T.StringType()), True),
      #     T.StructField("timezone", T.ArrayType(T.StringType()), True),
      #     T.StructField("tornadoDetection", T.ArrayType(T.StringType()), True),
      #     T.StructField("windThreat", T.ArrayType(T.StringType()), True),
      # ]), True),
      # T.StructField("references", T.ArrayType(T.StructType([
      #     # T.StructField("@id", T.StringType(), True),
      #     T.StructField("identifier", T.StringType(), True),
      #     T.StructField("sender", T.StringType(), True),
      #     T.StructField("sent", T.StringType(), True),
      # ])), True),

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
    .json(f"gs://weather_alerts_hlui/raw_data/{file_name}")
  
  df = df \
    .withColumn("geometry_wkt", wkt_udf("geometry.type", "geometry.coordinates")) \
    .withColumn("affectedZones",  F.expr("transform(properties.affectedZones, x -> x)")) \
    .withColumn("areaDesc", col("properties.areaDesc")) \
    .withColumn("category", col("properties.category")) \
    .withColumn("certainty", col("properties.certainty")) \
    .withColumn("description", col("properties.description")) \
    .withColumn("effective", F.to_timestamp(col("properties.effective"))) \
    .withColumn("ends",  F.to_timestamp(col("properties.ends"))) \
    .withColumn("event", col("properties.event")) \
    .withColumn("expires",  F.to_timestamp(col("properties.expires"))) \
    .withColumn("headline", col("properties.headline")) \
    .withColumn("id", col("properties.id")) \
    .withColumn("instruction", col("properties.instruction")) \
    .withColumn("messageType", col("properties.messageType")) \
    .withColumn("onset", col("properties.onset")) \
    .withColumn("replacedAt",  F.to_timestamp(col("properties.replacedAt"))) \
    .withColumn("replacedBy",  col("properties.replacedBy")) \
    .withColumn("response", col("properties.response")) \
    .withColumn("sender", col("properties.sender")) \
    .withColumn("senderName", col("properties.senderName")) \
    .withColumn("sent",  F.to_timestamp(col("properties.sent"))) \
    .withColumn("severity", col("properties.severity")) \
    .withColumn("status", col("properties.status")) \
    .withColumn("urgency", col("properties.urgency")) \
    .drop("geometry") \
    .drop("properties")

  df \
    .repartition(4) \
    .write.mode("overwrite") \
    .parquet(f"gs://weather_alerts_hlui/parquet/{ingestion_date}")
  
  df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "weather_alerts_hlui_temp") \
    .mode("overwrite") \
    .option("table", f'western-diorama-455122-u6.weather_staging.alerts_{ingestion_date}') \
    .save()
  
  spark.stop()

if __name__ == "__main__":
    main()



