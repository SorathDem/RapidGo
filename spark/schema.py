from pyspark.sql.types import StructType, StructField, StringType, DoubleType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("odometer_km", DoubleType()),
    StructField("km_since_last_service", DoubleType()),
    StructField("engine_temp_c", DoubleType()),
    StructField("fault_code", StringType()),
    StructField("usage_hours_today", DoubleType()),
])