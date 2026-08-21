from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# ============================================================
# 1. BRONZE SCHEMA
# ============================================================

bronze_schema = StructType([
    StructField("Body", StringType(), True),
    StructField("EnqueuedTimeUtc", StringType(), True)
])


# ============================================================
# 2. MOTOR JSON SCHEMA
# ============================================================

motor_schema = StructType([
    StructField("motor_id", StringType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("current", DoubleType(), True)
])


# ============================================================
# 3. CREATE SILVER DELTA SINK
# ============================================================

dp.create_sink(
    name="silver_motor_sink",
    format="delta",
    options={
        "tableName": "motordatabricks.silver.motor_telemetry"
    }
)


# ============================================================
# 4. BRONZE → SILVER FLOW
# ============================================================

@dp.append_flow(
    name="bronze_to_silver",
    target="silver_motor_sink"
)
def bronze_to_silver():

    df_bronze = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(bronze_schema)
        .option("recursiveFileLookup", "true")
        .load(
            "abfss://bronze@motorlakehrk.dfs.core.windows.net/"
        )
    )

    # Parse Body JSON
    df_parsed = (
        df_bronze
        .select(
            from_json(
                col("Body"),
                motor_schema
            ).alias("motor"),

            col("EnqueuedTimeUtc")
            .alias("enqueued_time_utc")
        )
    )

    # Flatten
    df_silver = (
        df_parsed
        .select(
            col("motor.motor_id").alias("motor_id"),
            col("motor.rpm").alias("rpm"),
            col("motor.temperature").alias("temperature"),
            col("motor.vibration").alias("vibration"),
            col("motor.current").alias("current"),

            to_timestamp(
                col("motor.timestamp")
            ).alias("event_timestamp"),

            to_timestamp(
                col("enqueued_time_utc")
            ).alias("enqueued_time_utc")
        )
    )

    # Data quality
    df_silver = (
        df_silver
        .filter(col("motor_id").isNotNull())
        .filter(col("rpm").isNotNull())
        .filter(col("temperature").isNotNull())
        .filter(col("event_timestamp").isNotNull())
    )

    return df_silver