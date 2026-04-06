"""Utility functions to read in and process Calgary transit data."""

from data_demos.process import calgary_transit_schemas
from data_demos.ingest import calgary_transit
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

SCHEMA_ID_MAP = {
    "vehicle_positions": {
        "id": calgary_transit.VEHICLE_POSITIONS_ID,
        "schema": calgary_transit_schemas.BRONZE_VEHICLE_POSITIONS_SCHEMA,
    },
    "service_alerts": {
        "id": calgary_transit.SERVICE_ALERTS_ID,
        "schema": calgary_transit_schemas.BRONZE_SERVICE_ALERTS_SCHEMA,
    },
    "trip_updates": {
        "id": calgary_transit.TRIP_UPDATES_ID,
        "schema": calgary_transit_schemas.BRONZE_TRIP_UPDATES_SCHEMA,
    },
}


def stream_cloud_files(source: str, spark: SparkSession) -> DataFrame:
    """Read in Calgary transit files with autoloader."""
    if source not in SCHEMA_ID_MAP.keys():
        raise RuntimeError(
            f"Invalid source: {source}, must be one of {', '.join(key for key in SCHEMA_ID_MAP.keys())}"
        )
    schema = SCHEMA_ID_MAP[source]["schema"]
    volume_path = (
        f"/Volumes/ian_preston/ingest/calgary_transit/{SCHEMA_ID_MAP[source]['id']}"
    )
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(schema)
        .load(volume_path)
    )
