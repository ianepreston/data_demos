from pyspark.sql import functions as F

df = (
    spark.sql("""
            select
            path,
            _metadata.file_name as file_name
            from read_files('/Volumes/ian_preston/ingest/calgary_transit/gs4m-mdc2/', format => 'binaryFile')
            """)
    .withColumn("clean_volume_path", F.regexp_replace(F.col("path"), "dbfs:", ""))
    .withColumn("file_ts", F.regexp_replace(F.col("file_name"), r"\..+", ""))
)
