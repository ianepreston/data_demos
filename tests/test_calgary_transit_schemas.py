"""Tests that Calgary Transit schemas match sample JSON data."""

import pathlib

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from data_demos.process.calgary_transit_schemas import (
    BRONZE_SERVICE_ALERTS_SCHEMA,
    BRONZE_TRIP_UPDATES_SCHEMA,
    BRONZE_VEHICLE_POSITIONS_SCHEMA,
)

SAMPLE_DATA_DIR = (
    pathlib.Path(__file__).resolve().parents[1] / "sample_data" / "calgary_transit"
)

SCHEMA_FILE_PAIRS = [
    (BRONZE_VEHICLE_POSITIONS_SCHEMA, "vehicle_positions.json"),
    (BRONZE_SERVICE_ALERTS_SCHEMA, "service_alerts.json"),
    (BRONZE_TRIP_UPDATES_SCHEMA, "trip_updates.json"),
]


def _normalize_schema(schema: StructType) -> StructType:
    """Normalize a schema for comparison.

    Recursively sorts fields by name and sets all nullable=True,
    since Spark inference always produces nullable fields in alphabetical order.
    """
    from pyspark.sql.types import ArrayType, StructField

    normalized = []
    for field in sorted(schema.fields, key=lambda f: f.name):
        dt = field.dataType
        if isinstance(dt, StructType):
            dt = _normalize_schema(dt)
        elif isinstance(dt, ArrayType) and isinstance(dt.elementType, StructType):
            dt = ArrayType(_normalize_schema(dt.elementType), containsNull=True)
        normalized.append(StructField(field.name, dt, nullable=True))
    return StructType(normalized)


@pytest.mark.parametrize(
    "defined_schema, filename",
    SCHEMA_FILE_PAIRS,
    ids=["vehicle_positions", "service_alerts", "trip_updates"],
)
def test_schema_matches_sample_data(
    local_spark: SparkSession, defined_schema: StructType, filename: str
):
    """Infer schema from sample JSON and verify it matches the defined schema."""
    path = str(SAMPLE_DATA_DIR / filename)
    inferred = local_spark.read.option("multiLine", True).json(path).schema
    assert _normalize_schema(defined_schema) == _normalize_schema(inferred), (
        f"Schema mismatch for {filename}.\n"
        f"Defined: {defined_schema.simpleString()}\n"
        f"Inferred: {inferred.simpleString()}"
    )
