"""Ingest Calgary Transit data.

This will get broken up later as I do different experiments but it's enough for now.
"""

# Let's just try a baby one to start and we'll go from there

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from data_demos.process.calgary_transit_schemas import BRONZE_VEHICLE_POSITIONS_SCHEMA
from data_demos.process.calgary_transit_ingest import stream_cloud_files

spark: SparkSession


@dp.table(
    name="bronze_vehicle_position",
    comment="Raw snapshots of vehicle position data. Taken every 30 seconds. No deduplication or unnesting performed.",
    cluster_by_auto=True,
    schema=BRONZE_VEHICLE_POSITIONS_SCHEMA,
)
def bronze_vehicle_position() -> DataFrame:
    return stream_cloud_files("vehicle_positions", spark)
