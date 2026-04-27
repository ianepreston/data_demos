"""Parse out Calgary Transit data from Bronze.

I want to experiment with doing auto CDC and then exploding vs exploding and then auto CDCing.
It might also be interesting to make a CDF type table to replicate an upstream CDC process.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types

spark: SparkSession


def unix_ts_to_edmonton(col_expr: F.Column) -> F.Column:
    """Convert a string Unix epoch column to a timestamp in America/Edmonton.

    Args:
        col_expr: A Column containing string-encoded Unix epoch seconds.

    Returns:
        Column: A timestamp column localized to America/Edmonton.
    """
    return F.from_utc_timestamp(
        col_expr.cast("long").cast("timestamp"), "America/Edmonton"
    )


def flatten_vehicle_position(table_name: str, spark: SparkSession) -> DataFrame:
    """Take the nested entity vehicle snapshot DataFrame and flatten it.

    Args:
        table_name: The fully upstream table/view to read from. If it's in the same schema doesn't have to be fully-qualified

    Returns:
        DataFrame: A Dataframe with a flattened schema
    """
    return (
        spark.read.table(table_name)
        .withColumn("snapshot_ts", unix_ts_to_edmonton(F.col("header.timestamp")))
        .withColumn("entities", F.explode(F.col("entity")))
        .withColumn("entity_id", F.col("entities.id"))
        .withColumn("trip_id", F.col("entities.vehicle.trip.tripId"))
        .withColumn("vehicle_id", F.col("entities.vehicle.vehicle.id"))
        .withColumn(
            "position_update_ts",
            unix_ts_to_edmonton(F.col("entities.vehicle.timestamp")),
        )
        .withColumn("vehicle_latitude", F.col("entities.vehicle.position.latitude"))
        .withColumn("vehicle_longitude", F.col("entities.vehicle.position.longitude"))
        .select(
            "entity_id",
            "trip_id",
            "vehicle_id",
            "snapshot_ts",
            "position_update_ts",
            "vehicle_latitude",
            "vehicle_longitude",
        )
    )


FLATTENED_VEHICLE_SCHEMA = """
    entity_id STRING COMMENT 'Unique identifier of an entity. Should be the same as trip ID in this table.',
    trip_id STRING COMMENT 'Unique identifier of a trip.',
    vehicle_id STRING COMMENT 'Unique identifier of a vehicle.',
    snapshot_ts TIMESTAMP COMMENT 'When the snapshot was produced in the data feed',
    position_update_ts TIMESTAMP COMMENT 'When the position update was sent',
    vehicle_latitude DOUBLE COMMENT 'The latitude of the vehicle at the time of position update',
    vehicle_longitude DOUBLE COMMENT 'The longitude of the vehicle at the time of position update'
"""


FLATTENED_VEHICLE_SCD2_SCHEMA = f"""
{FLATTENED_VEHICLE_SCHEMA},
__START_AT TIMESTAMP COMMENT "DateTime when the record is first valid",
__END_AT TIMESTAMP COMMENT "DateTime when the record is last valid (Null if still valid)"
"""


def next_snapshot_and_version(
    volume_path: str,
    schema: types.StructType | str,
    latest_snapshot: int | None,
) -> tuple[DataFrame, str] | None:
    full_file_list = (
        spark.sql(f"""
            select
            path,
            _metadata.file_name as file_name
            from read_files('{volume_path}', format => 'binaryFile')
            """)
        .withColumn("clean_volume_path", F.regexp_replace(F.col("path"), "dbfs:", ""))
        .withColumn("file_ts", F.regexp_replace(F.col("file_name"), r"\..+", ""))
        .withColumn("file_ts", F.col("file_ts").cast(types.IntegerType()))
    )
    if latest_snapshot:
        candidate_file_list = full_file_list.where(f"file_ts > {latest_snapshot}")
    else:
        candidate_file_list = full_file_list
    next_snapshot = candidate_file_list.agg(F.min("file_ts")).first()[0]
    if next_snapshot is None:
        return None
    full_volume_path = f"{volume_path}/{next_snapshot}.json"
    next_snapshot_df = spark.read.format("json").schema(schema).load(full_volume_path)
    return (next_snapshot_df, next_snapshot)
