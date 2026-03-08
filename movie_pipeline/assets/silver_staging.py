from dagster import MaterializeResult, MetadataValue, asset
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, IntegerType

from movie_pipeline.assets.bronze import bronze_links, bronze_movies, bronze_ratings, bronze_tags
from movie_pipeline.resources.s3_resource import S3Resource
from movie_pipeline.resources.spark_resource import SparkResource

SILVER_KINDS = {"spark", "delta"}
def _to_s3a(uri: str) -> str:
    return uri.replace("s3://", "s3a://", 1)

def _schema_meta(df: DataFrame) -> str:
    return "\n".join(f"  {f.name}: {f.dataType.simpleString()}" for f in df.schema.fields)

@asset(deps=[bronze_movies], group_name="silver", kinds=SILVER_KINDS, pool="spark")
def stg_movies(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    src = _to_s3a(f"s3://{bucket}/bronze/movies/movies.csv")
    dst = _to_s3a(f"s3://{bucket}/silver/staging/stg_movies")
    df = (
        spark.session.read.option("header", "true").option("inferSchema", "false").csv(src)
        .withColumnRenamed("movieId", "movie_id")
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))
    )
    row_count = df.count()
    df.write.format("delta").mode("overwrite").save(dst)
    context.log.info("stg_movies: %d rows → %s", row_count, dst)
    return MaterializeResult(metadata={"row_count": row_count, "s3_path": MetadataValue.text(dst), "schema": MetadataValue.text(_schema_meta(df))})

@asset(deps=[bronze_ratings], group_name="silver", kinds=SILVER_KINDS, pool="spark")
def stg_ratings(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    src = _to_s3a(f"s3://{bucket}/bronze/ratings/ratings.csv")
    dst = _to_s3a(f"s3://{bucket}/silver/staging/stg_ratings")
    df = (
        spark.session.read.option("header", "true").option("inferSchema", "false").csv(src)
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("movieId", "movie_id")
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))
        .withColumn("rating", F.col("rating").cast(DoubleType()))
        .withColumn("created_at", F.to_timestamp(F.col("timestamp").cast("long")))
        .drop("timestamp")
    )
    row_count = df.count()
    df.write.format("delta").mode("overwrite").save(dst)
    context.log.info("stg_ratings: %d rows → %s", row_count, dst)
    return MaterializeResult(metadata={"row_count": row_count, "s3_path": MetadataValue.text(dst), "schema": MetadataValue.text(_schema_meta(df))})

@asset(deps=[bronze_tags], group_name="silver", kinds=SILVER_KINDS, pool="spark")
def stg_tags(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    src = _to_s3a(f"s3://{bucket}/bronze/tags/tags.csv")
    dst = _to_s3a(f"s3://{bucket}/silver/staging/stg_tags")
    df = (
        spark.session.read.option("header", "true").option("inferSchema", "false").csv(src)
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("movieId", "movie_id")
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))
        .withColumn("tag", F.lower(F.trim(F.col("tag"))))
        .withColumn("created_at", F.to_timestamp(F.col("timestamp").cast("long")))
        .drop("timestamp")
        .filter(F.col("tag").isNotNull() & (F.col("tag") != ""))
    )
    row_count = df.count()
    df.write.format("delta").mode("overwrite").save(dst)
    context.log.info("stg_tags: %d rows → %s", row_count, dst)
    return MaterializeResult(metadata={"row_count": row_count, "s3_path": MetadataValue.text(dst), "schema": MetadataValue.text(_schema_meta(df))})

@asset(deps=[bronze_links], group_name="silver", kinds=SILVER_KINDS, pool="spark")
def stg_links(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    src = _to_s3a(f"s3://{bucket}/bronze/links/links.csv")
    dst = _to_s3a(f"s3://{bucket}/silver/staging/stg_links")
    df = (
        spark.session.read.option("header", "true").option("inferSchema", "false").csv(src)
        .withColumnRenamed("movieId", "movie_id")
        .withColumnRenamed("imdbId", "imdb_id")
        .withColumnRenamed("tmdbId", "tmdb_id")
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))
        .withColumn("tmdb_id", F.col("tmdb_id").cast(IntegerType()))
    )
    row_count = df.count()
    df.write.format("delta").mode("overwrite").save(dst)
    context.log.info("stg_links: %d rows → %s", row_count, dst)
    return MaterializeResult(metadata={"row_count": row_count, "s3_path": MetadataValue.text(dst), "schema": MetadataValue.text(_schema_meta(df))})
