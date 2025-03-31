CREATE_PROD_TABLE = """
  CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.weather_alerts`
  (
    id BYTES,
    geometry_wkt STRING,
    affectedZone STRING,
    areaDesc STRING,
    category STRING,
    certainty STRING,
    description STRING,
    effective TIMESTAMP,
    ends TIMESTAMP,
    event STRING,
    expires TIMESTAMP,
    headline STRING,
    alert_id STRING,
    instruction STRING,
    messageType STRING,
    onset STRING,
    replacedAt TIMESTAMP,
    replacedBy STRING,
    response STRING,
    sender STRING,
    senderName STRING,
    sent TIMESTAMP,
    severity STRING,
    status STRING,
    urgency STRING
  )
  PARTITION BY DATE(effective)
  CLUSTER BY affectedZone;
"""
