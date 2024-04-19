from pyspark.sql import SparkSession


def start_spark() -> SparkSession:
    builder = SparkSession.builder.config(
        "spark.jars.packages", "io.delta:delta-core_2.12:2.3.0"
    ) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]")

    spark_session = builder.getOrCreate()
    return spark_session
