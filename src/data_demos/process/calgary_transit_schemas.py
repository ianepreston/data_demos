"""Schemas for Calgary Transit Data.

Kind of annoying to navigate, easier to pull them into your code.
Covers Vehicle Positions, Trip Updates, Service Alerts
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    DoubleType,
    LongType,
)


def _build_schema_with_header(entity_schema: StructType) -> StructType:
    """Everything has the same header structure, so we can compose it easily."""
    return StructType(
        [
            StructField(
                "header",
                StructType(
                    [
                        StructField("gtfsRealtimeVersion", StringType(), True),
                        StructField("incrementality", StringType(), True),
                        StructField("timestamp", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("entity", ArrayType(entity_schema, True)),
        ]
    )


_BRONZE_VEHICLE_POSITION_ENTITY_SCHEMA = StructType(
    [
        # Trip ID, seems to always be the same as the nested tripId element
        StructField("id", StringType(), True),
        StructField(
            "vehicle",
            StructType(
                [
                    StructField(
                        "position",
                        StructType(
                            [
                                StructField("latitude", DoubleType(), True),
                                StructField("longitude", DoubleType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("timestamp", StringType(), True),
                    StructField(
                        "trip",
                        StructType([StructField("tripId", StringType(), True)]),
                        True,
                    ),
                    StructField(
                        "vehicle",
                        StructType([StructField("id", StringType(), True)]),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

_ACTIVE_PERIOD_SCHEMA = ArrayType(
    StructType(
        [
            StructField("end", StringType(), True),
            StructField("start", StringType(), True),
        ]
    ),
    True,
)

_INFORMED_ENTITY_SCHEMA = ArrayType(
    StructType(
        [
            StructField("agencyId", StringType(), True),
            StructField("routeId", StringType(), True),
            StructField("stopId", StringType(), True),
        ]
    ),
    True,
)

_HEADER_OR_DESCRIPTION_TEXT_SCHEMA = StructType(
    [
        StructField(
            "translation",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "language",
                            StringType(),
                            True,
                        ),
                        StructField(
                            "text",
                            StringType(),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            True,
        )
    ]
)

_BRONZE_SERVICE_ALERT_ENTITY_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField(
            "alert",
            StructType(
                [
                    StructField("activePeriod", _ACTIVE_PERIOD_SCHEMA),
                    StructField("informedEntity", _INFORMED_ENTITY_SCHEMA),
                    StructField("headerText", _HEADER_OR_DESCRIPTION_TEXT_SCHEMA),
                    StructField("descriptionText", _HEADER_OR_DESCRIPTION_TEXT_SCHEMA),
                ]
            ),
        ),
    ]
)
_BRONZE_STOP_TIME_UPDATE_SCHEMA = StructType(
    [
        StructField(
            "arrival",
            StructType(
                [
                    StructField(
                        "time",
                        StringType(),
                        True,
                    )
                ]
            ),
            True,
        ),
        StructField(
            "departure",
            StructType(
                [
                    StructField(
                        "time",
                        StringType(),
                        True,
                    )
                ]
            ),
            True,
        ),
        StructField(
            "scheduleRelationship",
            StringType(),
            True,
        ),
        StructField("stopId", StringType(), True),
        StructField("stopSequence", LongType(), True),
    ]
)

_BRONZE_TRIP_UPDATE_SCHEMA = StructType(
    [
        StructField(
            "trip",
            StructType(
                [
                    StructField("routeId", StringType(), True),
                    StructField(
                        "scheduleRelationship",
                        StringType(),
                        True,
                    ),
                    StructField("tripId", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "stopTimeUpdate", ArrayType(_BRONZE_STOP_TIME_UPDATE_SCHEMA, True), True
        ),
    ]
)
_BRONZE_TRIP_UPDATE_ENTITY_SCHEMA = StructType(
    [
        # This looks to be the same as tripID
        StructField("id", StringType(), True),
        StructField("tripUpdate", _BRONZE_TRIP_UPDATE_SCHEMA, True),
    ]
)


BRONZE_VEHICLE_POSITION_SCHEMA = _build_schema_with_header(
    _BRONZE_VEHICLE_POSITION_ENTITY_SCHEMA
)

BRONZE_SERVICE_ALERT_SCHEMA = _build_schema_with_header(
    _BRONZE_SERVICE_ALERT_ENTITY_SCHEMA
)

BRONZE_TRIP_UPDATE_SCHEMA = _build_schema_with_header(_BRONZE_TRIP_UPDATE_ENTITY_SCHEMA)
