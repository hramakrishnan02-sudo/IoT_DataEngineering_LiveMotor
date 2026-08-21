from pyspark import pipelines as dp
from pyspark.sql.functions import *
from delta.tables import DeltaTable


# ============================================================
# SOURCE
# ============================================================

SILVER_TABLE = "motordatabricks.silver.motor_telemetry"

CURRENT_STATUS_TABLE = "motordatabricks.gold.motor_current_status"

METRICS_TABLE = "motordatabricks.gold.motor_metrics"


# ============================================================
# 1. CURRENT STATUS SINK
# ============================================================

@dp.foreach_batch_sink(
    name="motor_current_status_sink"
)
def motor_current_status_sink(batch_df, batch_id):

    # Skip empty micro-batches
    if batch_df.isEmpty():
        return

    # Get latest reading for each motor in this batch
    latest = (
        batch_df
        .groupBy("motor_id")
        .agg(
            max_by(
                struct(
                    col("rpm"),
                    col("temperature"),
                    col("vibration"),
                    col("current"),
                    col("event_timestamp"),
                    col("enqueued_time_utc")
                ),
                col("event_timestamp")
            ).alias("latest")
        )
        .select(
            "motor_id",
            col("latest.rpm").alias("rpm"),
            col("latest.temperature").alias("temperature"),
            col("latest.vibration").alias("vibration"),
            col("latest.current").alias("current"),
            col("latest.event_timestamp").alias("event_timestamp"),
            col("latest.enqueued_time_utc").alias("enqueued_time_utc")
        )
    )

    # Calculate health status
    latest = (
        latest
        .withColumn(
            "health_status",
            when(
                (col("temperature") > 90) |
                (col("vibration") > 8),
                "CRITICAL"
            )
            .when(
                (col("temperature") > 75) |
                (col("vibration") > 5),
                "WARNING"
            )
            .otherwise("NORMAL")
        )
    )

    # Access existing external Delta table
    target = DeltaTable.forName(
        batch_df.sparkSession,
        CURRENT_STATUS_TABLE
    )

    # Upsert latest status for each motor
    (
        target.alias("target")
        .merge(
            latest.alias("source"),
            "target.motor_id = source.motor_id"
        )

        .whenMatchedUpdate(
            condition="""
                source.event_timestamp > target.event_timestamp
            """,
            set={
                "rpm": "source.rpm",
                "temperature": "source.temperature",
                "vibration": "source.vibration",
                "current": "source.current",
                "event_timestamp": "source.event_timestamp",
                "enqueued_time_utc": "source.enqueued_time_utc",
                "health_status": "source.health_status"
            }
        )

        .whenNotMatchedInsert(
            values={
                "motor_id": "source.motor_id",
                "rpm": "source.rpm",
                "temperature": "source.temperature",
                "vibration": "source.vibration",
                "current": "source.current",
                "event_timestamp": "source.event_timestamp",
                "enqueued_time_utc": "source.enqueued_time_utc",
                "health_status": "source.health_status"
            }
        )

        .execute()
    )


# ============================================================
# 2. SILVER → CURRENT STATUS FLOW
# ============================================================

@dp.append_flow(
    name="silver_to_current_status",
    target="motor_current_status_sink"
)
def silver_to_current_status():

    return (
        spark.readStream
        .table(
            "motordatabricks.silver.motor_telemetry"
        )
    )


# ============================================================
# 3. MOTOR METRICS SINK
# ============================================================

@dp.foreach_batch_sink(
    name="motor_metrics_sink"
)
def motor_metrics_sink(batch_df, batch_id):

    target = DeltaTable.forName(
        batch_df.sparkSession,
        METRICS_TABLE
    )

    (
        target.alias("target")
        .merge(
            batch_df.alias("source"),
            """
            target.motor_id = source.motor_id
            AND target.window_start = source.window_start
            AND target.window_end = source.window_end
            """
        )
        .whenMatchedUpdate(
            set={
                "avg_temperature": "source.avg_temperature",
                "max_temperature": "source.max_temperature",
                "avg_vibration": "source.avg_vibration",
                "max_vibration": "source.max_vibration",
                "avg_rpm": "source.avg_rpm",
                "avg_current": "source.avg_current",
                "reading_count": "source.reading_count"
            }
        )
        .whenNotMatchedInsert(
            values={
                "motor_id": "source.motor_id",
                "window_start": "source.window_start",
                "window_end": "source.window_end",
                "avg_temperature": "source.avg_temperature",
                "max_temperature": "source.max_temperature",
                "avg_vibration": "source.avg_vibration",
                "max_vibration": "source.max_vibration",
                "avg_rpm": "source.avg_rpm",
                "avg_current": "source.avg_current",
                "reading_count": "source.reading_count"
            }
        )
        .execute()
    )


# ============================================================
# 4. SILVER → MOTOR METRICS FLOW
# ============================================================

@dp.append_flow(
    name="silver_to_motor_metrics",
    target="motor_metrics_sink"
)
def silver_to_motor_metrics():

    df = (
        spark.readStream
        .table(SILVER_TABLE)

        # Allow late-arriving IoT events
        .withWatermark(
            "event_timestamp",
            "2 minutes"
        )

        # 5-minute aggregation
        .groupBy(
            "motor_id",
            window(
                "event_timestamp",
                "5 minutes"
            )
        )
        .agg(
            avg("temperature").alias("avg_temperature"),
            max("temperature").alias("max_temperature"),

            avg("vibration").alias("avg_vibration"),
            max("vibration").alias("max_vibration"),

            avg("rpm").alias("avg_rpm"),

            avg("current").alias("avg_current"),

            count("*").alias("reading_count")
        )
        .select(
            "motor_id",

            col("window.start")
            .alias("window_start"),

            col("window.end")
            .alias("window_end"),

            "avg_temperature",
            "max_temperature",

            "avg_vibration",
            "max_vibration",

            "avg_rpm",
            "avg_current",

            "reading_count"
        )
    )

    return df