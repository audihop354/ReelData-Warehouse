from dagster import MaterializeResult, MetadataValue, asset
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DoubleType, IntegerType

from movie_pipeline.assets.silver_staging import stg_links, stg_movies, stg_ratings, stg_tags
from movie_pipeline.partitions import yearly_partitions
from movie_pipeline.resources.s3_resource import S3Resource
from movie_pipeline.resources.spark_resource import SparkResource

SILVER_KINDS = {"spark", "delta"}

def _to_s3a(uri: str) -> str:
    return uri.replace("s3://", "s3a://", 1)
def _schema_meta(df: DataFrame) -> str:
    return "\n".join(f"  {f.name}: {f.dataType.simpleString()}" for f in df.schema.fields)
def _persist_full_refresh(df: DataFrame, dst: str) -> int:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dst)
    return df.count()

def _delta_merge(spark, dst: str, updates, merge_condition: str) -> int:
    if DeltaTable.isDeltaTable(spark.session, dst):
        (
            DeltaTable.forPath(spark.session, dst)
            .alias("tgt")
            .merge(updates.alias("src"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return DeltaTable.forPath(spark.session, dst).toDF().count()
    else:
        updates.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dst)
        return updates.count()

@asset(deps=[stg_movies, stg_links], group_name="silver", kinds=SILVER_KINDS, pool="spark")
def dim_movies(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    movies_src = _to_s3a(f"s3://{bucket}/silver/staging/stg_movies")
    links_src = _to_s3a(f"s3://{bucket}/silver/staging/stg_links")
    dst = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_movies")

    movies = spark.session.read.format("delta").load(movies_src)
    links = spark.session.read.format("delta").load(links_src)

    normalized_links = (
        links
        .withColumn(
            "imdb_id",
            F.when(
                F.col("imdb_id").isNotNull(),
                F.concat(F.lit("tt"), F.regexp_replace(F.col("imdb_id"), r"^tt", "")),
            ),
        )
    )

    df = (
        movies
        .join(normalized_links, on="movie_id", how="left")
        .withColumn("year", F.regexp_extract(F.col("title"), r"\((\d{4})\)\s*$", 1).cast(IntegerType()))
        .withColumn("genres", F.coalesce(F.col("genres"), F.lit("(no genres listed)")))
        .withColumn("genres", F.explode(F.split(F.col("genres"), r"\|")))
        .withColumn("genres", F.lower(F.trim(F.col("genres"))))
        .filter(F.col("genres") != "")
        .withColumn("genre_count", F.count("*").over(Window.partitionBy("movie_id")))
        .withColumn(
            "imdb_url",
            F.when(F.col("imdb_id").isNotNull(), F.concat(F.lit("https://www.imdb.com/title/"), F.col("imdb_id"), F.lit("/"))),
        )
        .withColumn(
            "tmdb_url",
            F.when(F.col("tmdb_id").isNotNull(), F.concat(F.lit("https://www.themoviedb.org/movie/"), F.col("tmdb_id"))),
        )
        .select("movie_id", "title", "year", "genres", "genre_count", "imdb_id", "imdb_url", "tmdb_id", "tmdb_url")
    )
    row_count = _persist_full_refresh(df, dst)
    context.log.info("dim_movies: %d rows → %s", row_count, dst)
    return MaterializeResult(metadata={"row_count": row_count, "s3_path": MetadataValue.text(dst), "schema": MetadataValue.text(_schema_meta(df))})

@asset(deps=[stg_ratings, stg_tags], group_name="silver", kinds=SILVER_KINDS, pool="spark")
def dim_users(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    ratings_src = _to_s3a(f"s3://{bucket}/silver/staging/stg_ratings")
    tags_src = _to_s3a(f"s3://{bucket}/silver/staging/stg_tags")
    dst = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_users")
    rating_users = spark.session.read.format("delta").load(ratings_src).select("user_id")
    tag_users = spark.session.read.format("delta").load(tags_src).select("user_id")
    df = (
        rating_users.union(tag_users)
        .distinct()
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .orderBy("user_id")
    )
    row_count = _persist_full_refresh(df, dst)
    context.log.info("dim_users: %d rows → %s", row_count, dst)
    return MaterializeResult(metadata={"row_count": row_count, "s3_path": MetadataValue.text(dst), "schema": MetadataValue.text(_schema_meta(df))})

@asset(
    deps=[stg_ratings, dim_movies, dim_users],
    group_name="silver",
    kinds=SILVER_KINDS,
    partitions_def=yearly_partitions,
    pool="spark",
)
def fact_ratings(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    ratings_src = _to_s3a(f"s3://{bucket}/silver/staging/stg_ratings")
    movies_src = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_movies")
    users_src = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_users")
    dst = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_ratings")
    partition_year = int(context.partition_key)
    ratings = (
        spark.session.read.format("delta").load(ratings_src)
        .withColumn("rating", F.col("rating").cast(DoubleType()))
        .withColumn("year", F.year(F.col("created_at")).cast(IntegerType()))
        .filter(F.col("rating").between(0.5, 5.0))
        .filter(F.col("year") == partition_year)
    )
    dim_m = spark.session.read.format("delta").load(movies_src).select("movie_id").distinct()
    dim_u = spark.session.read.format("delta").load(users_src).select("user_id")
    df = (
        ratings
        .join(dim_m, on="movie_id", how="left_semi")
        .join(dim_u, on="user_id", how="inner")
        .select("user_id", "movie_id", "rating", "created_at", "year")
    )
    merge_condition = (
        "tgt.user_id = src.user_id "
        "AND tgt.movie_id = src.movie_id "
        "AND tgt.created_at = src.created_at"
    )
    row_count = _delta_merge(spark, dst, df, merge_condition)
    context.log.info("fact_ratings [year=%d]: %d rows → %s", partition_year, row_count, dst)
    return MaterializeResult(metadata={
        "row_count": row_count,
        "partition_year": partition_year,
        "s3_path": MetadataValue.text(dst),
        "schema": MetadataValue.text(_schema_meta(df)),
    })

@asset(deps=[stg_tags, dim_movies, dim_users], group_name="silver", kinds=SILVER_KINDS, pool="spark")
def fact_tags(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    tags_src = _to_s3a(f"s3://{bucket}/silver/staging/stg_tags")
    movies_src = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_movies")
    users_src = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_users")
    dst = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_tags")
    tags = (
        spark.session.read.format("delta").load(tags_src)
        .withColumn("year", F.year(F.col("created_at")).cast(IntegerType()))
    )
    dim_m = spark.session.read.format("delta").load(movies_src).select("movie_id").distinct()
    dim_u = spark.session.read.format("delta").load(users_src).select("user_id")
    df = (
        tags
        .join(dim_m, on="movie_id", how="left_semi")
        .join(dim_u, on="user_id", how="inner")
        .select("user_id", "movie_id", "tag", "created_at", "year")
    )
    row_count = _persist_full_refresh(df, dst)
    context.log.info("fact_tags: %d rows → %s", row_count, dst)
    return MaterializeResult(metadata={"row_count": row_count, "s3_path": MetadataValue.text(dst), "schema": MetadataValue.text(_schema_meta(df))})

@asset(deps=[fact_ratings, dim_movies], group_name="silver", kinds=SILVER_KINDS, pool="spark")
def fact_movie_genre_ratings(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    ratings_src = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_ratings")
    movies_src = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_movies")
    dst = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_movie_genre_ratings")

    ratings = spark.session.read.format("delta").load(ratings_src).select("movie_id", "rating")
    movies = spark.session.read.format("delta").load(movies_src).select(
        "movie_id",
        "title",
        F.col("year").alias("movie_year"),
        "genres",
    )
    df = (
        ratings
        .join(movies, on="movie_id", how="inner")
        .groupBy("movie_id", "title", "movie_year", "genres")
        .agg(
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating"),
            F.min("rating").alias("min_rating"),
            F.max("rating").alias("max_rating"),
        )
        .withColumnRenamed("movie_year", "year")
    )
    row_count = _persist_full_refresh(df, dst)
    context.log.info("fact_movie_genre_ratings: %d rows → %s", row_count, dst)
    return MaterializeResult(metadata={"row_count": row_count, "s3_path": MetadataValue.text(dst), "schema": MetadataValue.text(_schema_meta(df))})