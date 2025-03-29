
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime


def main():

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
      T.StructField("@id", T.StringType(), True),
      T.StructField("@type", T.StringType(), True),
      T.StructField("affectedZones", T.ArrayType(T.StringType()), True),
      T.StructField("areaDesc", T.StringType(), True),
      T.StructField("category", T.StringType(), True),
      T.StructField("certainty", T.StringType(), True),
      T.StructField("description", T.StringType(), True),
      T.StructField("effective", T.StringType(), True),
      T.StructField("ends", T.StringType(), True),
      T.StructField("event", T.StringType(), True),
      T.StructField("expires", T.StringType(), True),
      T.StructField("geocode", T.StructType([
          T.StructField("SAME", T.ArrayType(T.StringType()), True),
          T.StructField("UGC", T.ArrayType(T.StringType()), True),
      ]), True),
      T.StructField("headline", T.StringType(), True),
      T.StructField("id", T.StringType(), True),
      T.StructField("instruction", T.StringType(), True),
      T.StructField("messageType", T.StringType(), True),
      T.StructField("onset", T.StringType(), True),
      T.StructField("parameters", T.StructType([
          T.StructField("AWIPSidentifier", T.ArrayType(T.StringType()), True),
          T.StructField("BLOCKCHANNEL", T.ArrayType(T.StringType()), True),
          T.StructField("CMAMlongtext", T.ArrayType(T.StringType()), True),
          T.StructField("CMAMtext", T.ArrayType(T.StringType()), True),
          T.StructField("EAS-ORG", T.ArrayType(T.StringType()), True),
          T.StructField("NWSheadline", T.ArrayType(T.StringType()), True),
          T.StructField("VTEC", T.ArrayType(T.StringType()), True),
          T.StructField("WEAHandling", T.ArrayType(T.StringType()), True),
          T.StructField("WMOidentifier", T.ArrayType(T.StringType()), True),
          T.StructField("eventEndingTime", T.ArrayType(T.StringType()), True),
          T.StructField("eventMotionDescription", T.ArrayType(T.StringType()), True),
          T.StructField("expiredReferences", T.ArrayType(T.StringType()), True),
          T.StructField("flashFloodDetection", T.ArrayType(T.StringType()), True),
          T.StructField("hailThreat", T.ArrayType(T.StringType()), True),
          T.StructField("maxHailSize", T.ArrayType(T.StringType()), True),
          T.StructField("maxWindGust", T.ArrayType(T.StringType()), True),
          T.StructField("thunderstormDamageThreat", T.ArrayType(T.StringType()), True),
          T.StructField("timezone", T.ArrayType(T.StringType()), True),
          T.StructField("tornadoDetection", T.ArrayType(T.StringType()), True),
          T.StructField("windThreat", T.ArrayType(T.StringType()), True),
      ]), True),

      T.StructField("references", T.ArrayType(T.StructType([
          T.StructField("@id", T.StringType(), True),
          T.StructField("identifier", T.StringType(), True),
          T.StructField("sender", T.StringType(), True),
          T.StructField("sent", T.StringType(), True),
      ])), True),

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

  df = spark.read \
    .option("multiline", "true") \
    .schema(data_schema) \
    .json(f"gs://weather_alerts_hlui/raw_data/{file_name}")
  df \
    .repartition(4) \
    .write.mode("overwrite") \
    .parquet(f"gs://weather_alerts_hlui/parquet/{file_name_parquet}")
  
  spark.stop()

if __name__ == "__main__":
    main()



