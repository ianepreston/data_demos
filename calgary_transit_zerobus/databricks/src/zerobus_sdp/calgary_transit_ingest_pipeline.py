"""Ingest Calgary Transit data.

This will get broken up later as I do different experiments but it's enough for now.
"""

# Let's just try a baby one to start and we'll go from there

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from data_demos.process import calgary_transit_schemas
from data_demos.process.calgary_transit_ingest import stream_cloud_files

spark: SparkSession


@dp.table(
    name="bronze_vehicle_position",
    comment="SCD2 of vehicle positions taken from JSON snapshots",
    # cluster_by_auto=True,
    schema=calgary_transit_schemas.BRONZE_VEHICLE_POSITIONS_SCD2_SCHEMA,
)
def bronze_vehicle_position() -> DataFrame:
    return stream_cloud_files("vehicle_positions", spark)


@dp.temporary_view(name="flattened_vehicle_position")
def flatten_vehicle_position_view() -> DataFrame:
    return calgary_transit_process.flatten_vehicle_position(
        "bronze_vehicle_position", spark
    )


dp.create_streaming_table(
    name="vehicle_position_scd_2",
    comment="Current and historical vehicle positions",
    # cluster_by_auto=True,
    schema=calgary_transit_schemas.BRONZE_VEHICLE_POSITIONS_SCD2_SCHEMA,
)

dp.create_auto_cdc_from_snapshot_flow(
    target="gold_vehicle_position_scd_2",
    source=next_snapshot_and_version,
    keys=["trip_id", "vehicle_id"],
    stored_as_scd_type=2,
    track_history_except_column_list=None,
)
