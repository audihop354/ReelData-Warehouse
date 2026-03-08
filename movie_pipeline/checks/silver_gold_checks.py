from dagster import AssetCheckResult, MetadataValue, asset_check
from pyspark.sql import functions as F

from movie_pipeline.assets.gold_marts import (
    mart_genre_performance,
    mart_movie_analytics,
    mart_user_behavior,
)
from movie_pipeline.assets.silver_intermediate import (
    dim_movies,
    fact_movie_genre_ratings,
    fact_ratings,
    fact_tags,
)
from movie_pipeline.resources.s3_resource import S3Resource
from movie_pipeline.resources.spark_resource import SparkResource

def _to_s3a(uri: str) -> str:
    return uri.replace("s3://", "s3a://", 1)
def _delta_path(bucket: str, layer: str, name: str) -> str:
    return _to_s3a(f"s3://{bucket}/{layer}/{name}")

@asset_check(asset=dim_movies)
def dim_movies_genre_count_consistent(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    movies = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "dim_movies"))

    inconsistent_movie_count = (
        movies.groupBy("movie_id")
        .agg(
            F.countDistinct("genres").alias("actual_genre_count"),
            F.min("genre_count").alias("min_genre_count"),
            F.max("genre_count").alias("max_genre_count"),
        )
        .filter(
            (F.col("actual_genre_count") != F.col("min_genre_count"))
            | (F.col("min_genre_count") != F.col("max_genre_count"))
        )
        .count()
    )

    return AssetCheckResult(
        passed=inconsistent_movie_count == 0,
        description="dim_movies.genre_count should match the number of genre rows per movie.",
        metadata={"inconsistent_movie_count": inconsistent_movie_count},
    )

@asset_check(asset=fact_ratings)
def fact_ratings_has_unique_business_key(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    ratings = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_ratings"))

    total_rows = ratings.count()
    distinct_rows = ratings.select("user_id", "movie_id", "created_at").distinct().count()

    return AssetCheckResult(
        passed=total_rows == distinct_rows,
        description="fact_ratings should be unique on (user_id, movie_id, created_at).",
        metadata={
            "total_rows": total_rows,
            "distinct_business_keys": distinct_rows,
            "duplicate_rows": total_rows - distinct_rows,
        },
    )

@asset_check(asset=fact_ratings)
def fact_ratings_has_valid_movie_keys(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    ratings = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_ratings"))
    movies = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "dim_movies")).select("movie_id").distinct()

    orphan_count = ratings.join(movies, on="movie_id", how="left_anti").count()

    return AssetCheckResult(
        passed=orphan_count == 0,
        description="fact_ratings should only reference movie_ids present in dim_movies.",
        metadata={"orphan_movie_references": orphan_count},
    )

@asset_check(asset=fact_tags)
def fact_tags_has_valid_user_and_movie_keys(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    tags = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_tags"))
    movies = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "dim_movies")).select("movie_id").distinct()
    users = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "dim_users")).select("user_id").distinct()

    orphan_movie_count = tags.join(movies, on="movie_id", how="left_anti").count()
    orphan_user_count = tags.join(users, on="user_id", how="left_anti").count()

    return AssetCheckResult(
        passed=orphan_movie_count == 0 and orphan_user_count == 0,
        description="fact_tags should only reference keys present in dim_movies and dim_users.",
        metadata={
            "orphan_movie_references": orphan_movie_count,
            "orphan_user_references": orphan_user_count,
        },
    )

@asset_check(asset=fact_movie_genre_ratings)
def fact_movie_genre_ratings_preserves_exploded_rating_totals(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    ratings = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_ratings")).select("movie_id")
    movies = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "dim_movies")).select("movie_id", "genres")
    genre_ratings = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_movie_genre_ratings"))

    expected_total = ratings.join(movies, on="movie_id", how="inner").count()
    actual_total = genre_ratings.agg(F.sum("rating_count").alias("total")).collect()[0][0] or 0

    return AssetCheckResult(
        passed=expected_total == actual_total,
        description="fact_movie_genre_ratings should preserve total exploded rating counts from fact_ratings x dim_movies.",
        metadata={
            "expected_exploded_rating_total": expected_total,
            "actual_rating_count_total": actual_total,
        },
    )

@asset_check(asset=mart_movie_analytics)
def mart_movie_analytics_matches_distinct_movies(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    ratings = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_ratings"))
    mart = spark.session.read.format("delta").load(_delta_path(bucket, "gold/marts", "movie_analytics"))

    expected_rows = ratings.select("movie_id").distinct().count()
    actual_rows = mart.count()
    passed = actual_rows == expected_rows

    return AssetCheckResult(
        passed=passed,
        description="movie analytics row count should match distinct rated movies.",
        metadata={"expected_rows": expected_rows, "actual_rows": actual_rows},
    )

@asset_check(asset=mart_movie_analytics)
def mart_movie_analytics_preserves_rating_totals(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    ratings = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_ratings"))
    mart = spark.session.read.format("delta").load(_delta_path(bucket, "gold/marts", "movie_analytics"))

    expected_total = ratings.count()
    actual_total = mart.agg({"total_ratings": "sum"}).collect()[0][0] or 0
    passed = actual_total == expected_total

    return AssetCheckResult(
        passed=passed,
        description="sum(total_ratings) in movie analytics should equal fact_ratings row count.",
        metadata={
            "expected_total_ratings": expected_total,
            "actual_total_ratings": actual_total,
        },
    )

@asset_check(asset=mart_user_behavior)
def mart_user_behavior_matches_distinct_users(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    ratings = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_ratings"))
    mart = spark.session.read.format("delta").load(_delta_path(bucket, "gold/marts", "user_behavior"))

    expected_rows = ratings.select("user_id").distinct().count()
    actual_rows = mart.count()
    passed = actual_rows == expected_rows

    return AssetCheckResult(
        passed=passed,
        description="user behavior row count should match distinct users with ratings.",
        metadata={"expected_rows": expected_rows, "actual_rows": actual_rows},
    )

@asset_check(asset=mart_genre_performance)
def mart_genre_performance_matches_distinct_genres(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    genre_ratings = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_movie_genre_ratings"))
    mart = spark.session.read.format("delta").load(_delta_path(bucket, "gold/marts", "genre_performance"))

    expected_rows = genre_ratings.select("genres").distinct().count()
    actual_rows = mart.count()
    passed = actual_rows == expected_rows

    return AssetCheckResult(
        passed=passed,
        description="genre performance row count should match distinct genres in fact_movie_genre_ratings.",
        metadata={"expected_rows": expected_rows, "actual_rows": actual_rows},
    )

@asset_check(asset=mart_genre_performance)
def mart_genre_performance_has_expected_genre_set(spark: SparkResource, s3: S3Resource) -> AssetCheckResult:
    bucket = s3.require_bucket()
    genre_ratings = spark.session.read.format("delta").load(_delta_path(bucket, "silver/intermediate", "fact_movie_genre_ratings"))
    mart = spark.session.read.format("delta").load(_delta_path(bucket, "gold/marts", "genre_performance"))

    expected_genres = {row[0] for row in genre_ratings.select("genres").distinct().collect()}
    actual_genres = {row[0] for row in mart.select("genres").distinct().collect()}
    missing_genres = sorted(expected_genres - actual_genres)
    unexpected_genres = sorted(actual_genres - expected_genres)
    passed = not missing_genres and not unexpected_genres

    return AssetCheckResult(
        passed=passed,
        description="genre performance should contain the same genre set as fact_movie_genre_ratings.",
        metadata={
            "missing_genres": MetadataValue.json(missing_genres),
            "unexpected_genres": MetadataValue.json(unexpected_genres),
            "expected_genre_count": len(expected_genres),
            "actual_genre_count": len(actual_genres),
        },
    )