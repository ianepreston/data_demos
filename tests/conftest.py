import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def local_spark():
    """Create a single local Spark session shared across all tests."""
    session = (
        SparkSession.builder.master("local[*]")
        .appName("schema-tests")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    yield session
    session.stop()
