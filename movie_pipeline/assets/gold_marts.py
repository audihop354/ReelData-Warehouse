from dagster import MaterializeResult, MetadataValue, asset
from pyspark.sql import DataFrame, Window, functions as F

from movie_pipeline.assets.silver_intermediate import (
    dim_movies,
    dim_users,
    fact_movie_genre_ratings,
    fact_ratings,
    fact_tags,
)
from movie_pipeline.resources.s3_resource import S3Resource
from movie_pipeline.resources.spark_resource import SparkResource

GOLD_KINDS = {"spark", "delta"}

def _to_s3a(uri: str) -> str:
    return uri.replace("s3://", "s3a://", 1)

def _schema_meta(df: DataFrame) -> str:
    return "\n".join(f"  {field.name}: {field.dataType.simpleString()}" for field in df.schema.fields)

def _persist_full_refresh(df: DataFrame, dst: str) -> int:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dst)
    return df.count()


@asset(
    deps=[fact_ratings, fact_tags, dim_movies],
    group_name="gold",
    kinds=GOLD_KINDS,
    pool="spark",
)
def mart_movie_analytics(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    ratings_src = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_ratings")
    tags_src = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_tags")
    movies_src = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_movies")
    dst = _to_s3a(f"s3://{bucket}/gold/marts/movie_analytics")

    ratings = spark.session.read.format("delta").load(ratings_src)
    tags = spark.session.read.format("delta").load(tags_src)
    movies = spark.session.read.format("delta").load(movies_src)

    movie_info = movies.groupBy("movie_id").agg(
        F.first("title", ignorenulls=True).alias("title"),
        F.first("year", ignorenulls=True).alias("year"),
        F.concat_ws(", ", F.sort_array(F.collect_set("genres"))).alias("all_genres"),
        F.countDistinct("genres").alias("genre_count"),
    )

    ratings_agg = ratings.groupBy("movie_id").agg(
        F.count("*").alias("total_ratings"),
        F.avg("rating").alias("avg_rating"),
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
        F.stddev("rating").alias("stddev_rating"),
        F.min("created_at").alias("first_rating_date"),
        F.max("created_at").alias("last_rating_date"),
    )

    tags_agg = tags.groupBy("movie_id").agg(
        F.count("*").alias("total_tags"),
        F.countDistinct("user_id").alias("unique_taggers"),
        F.min("created_at").alias("first_tag_date"),
        F.max("created_at").alias("last_tag_date"),
    )

    df = (
        movie_info.join(ratings_agg, on="movie_id", how="inner")
        .join(tags_agg, on="movie_id", how="left")
        .fillna({"total_tags": 0, "unique_taggers": 0})
        .withColumn(
            "rating_span_days",
            F.datediff(F.col("last_rating_date"), F.col("first_rating_date")),
        )
        .withColumn(
            "engagement_tier",
            F.when(F.col("total_ratings") >= 100, F.lit("Highly Rated"))
            .when(F.col("total_ratings") >= 50, F.lit("Well Rated"))
            .when(F.col("total_ratings") >= 10, F.lit("Moderately Rated"))
            .otherwise(F.lit("Limited Ratings")),
        )
        .withColumn(
            "quality_tier",
            F.when(F.col("avg_rating") >= 4.0, F.lit("Excellent"))
            .when(F.col("avg_rating") >= 3.5, F.lit("Good"))
            .when(F.col("avg_rating") >= 3.0, F.lit("Average"))
            .when(F.col("avg_rating") >= 2.0, F.lit("Below Average"))
            .otherwise(F.lit("Poor")),
        )
        .withColumn("refreshed_at", F.current_timestamp())
        .select(
            "movie_id",
            "title",
            "year",
            "all_genres",
            "genre_count",
            "total_ratings",
            "avg_rating",
            "min_rating",
            "max_rating",
            "stddev_rating",
            "first_rating_date",
            "last_rating_date",
            "rating_span_days",
            "total_tags",
            "unique_taggers",
            "first_tag_date",
            "last_tag_date",
            "engagement_tier",
            "quality_tier",
            "refreshed_at",
        )
    )

    row_count = _persist_full_refresh(df, dst)
    context.log.info("mart_movie_analytics: %d rows -> %s", row_count, dst)
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "s3_path": MetadataValue.text(dst),
            "schema": MetadataValue.text(_schema_meta(df)),
        }
    )

@asset(
    deps=[fact_ratings, fact_tags, dim_movies, dim_users],
    group_name="gold",
    kinds=GOLD_KINDS,
    pool="spark",
)
def mart_user_behavior(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    ratings_src = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_ratings")
    tags_src = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_tags")
    movies_src = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_movies")
    users_src = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_users")
    dst = _to_s3a(f"s3://{bucket}/gold/marts/user_behavior")

    ratings = spark.session.read.format("delta").load(ratings_src)
    tags = spark.session.read.format("delta").load(tags_src)
    movies = spark.session.read.format("delta").load(movies_src).select("movie_id", "genres")
    users = spark.session.read.format("delta").load(users_src).select("user_id")

    rating_agg = ratings.groupBy("user_id").agg(
        F.count("*").alias("total_ratings"),
        F.countDistinct("movie_id").alias("unique_movies_rated"),
        F.avg("rating").alias("avg_rating_given"),
        F.min("rating").alias("min_rating_given"),
        F.max("rating").alias("max_rating_given"),
        F.stddev("rating").alias("stddev_rating"),
        F.min("created_at").alias("first_rating_date"),
        F.max("created_at").alias("last_rating_date"),
        F.countDistinct(F.to_date("created_at")).alias("active_days"),
    )

    tag_agg = tags.groupBy("user_id").agg(
        F.count("*").alias("total_tags"),
        F.countDistinct("movie_id").alias("unique_movies_tagged"),
        F.min("created_at").alias("first_tag_date"),
        F.max("created_at").alias("last_tag_date"),
    )

    preferred_genre = (
        ratings.join(movies, on="movie_id", how="inner")
        .groupBy("user_id", "genres")
        .agg(
            F.count("*").alias("preferred_genre_count"),
            F.avg("rating").alias("preferred_genre_avg_rating"),
        )
        .withColumn(
            "genre_rank",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(
                    F.col("preferred_genre_count").desc(),
                    F.col("preferred_genre_avg_rating").desc(),
                    F.col("genres").asc(),
                )
            ),
        )
        .filter(F.col("genre_rank") == 1)
        .select(
            "user_id",
            F.col("genres").alias("preferred_genre"),
            "preferred_genre_count",
            "preferred_genre_avg_rating",
        )
    )

    df = (
        users.join(rating_agg, on="user_id", how="inner")
        .join(tag_agg, on="user_id", how="left")
        .join(preferred_genre, on="user_id", how="left")
        .fillna({"total_tags": 0, "unique_movies_tagged": 0})
        .withColumn(
            "user_segment",
            F.when(F.col("total_ratings") >= 1000, F.lit("Power"))
            .when(F.col("total_ratings") >= 500, F.lit("Heavy"))
            .when(F.col("total_ratings") >= 100, F.lit("Active"))
            .when(F.col("total_ratings") >= 20, F.lit("Regular"))
            .otherwise(F.lit("Light")),
        )
        .withColumn(
            "rating_style",
            F.when(F.col("avg_rating_given") >= 4.0, F.lit("Positive Rater"))
            .when(F.col("avg_rating_given") >= 3.0, F.lit("Neutral Rater"))
            .otherwise(F.lit("Critical Rater")),
        )
        .withColumn(
            "avg_ratings_per_day",
            F.when(F.col("active_days") > 0, F.col("total_ratings") / F.col("active_days")).otherwise(F.lit(0.0)),
        )
        .withColumn("refreshed_at", F.current_timestamp())
        .select(
            "user_id",
            "total_ratings",
            "unique_movies_rated",
            "avg_rating_given",
            "min_rating_given",
            "max_rating_given",
            "stddev_rating",
            "first_rating_date",
            "last_rating_date",
            "active_days",
            "total_tags",
            "unique_movies_tagged",
            "first_tag_date",
            "last_tag_date",
            "preferred_genre",
            "preferred_genre_count",
            "preferred_genre_avg_rating",
            "user_segment",
            "rating_style",
            "avg_ratings_per_day",
            "refreshed_at",
        )
    )

    row_count = _persist_full_refresh(df, dst)
    context.log.info("mart_user_behavior: %d rows -> %s", row_count, dst)
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "s3_path": MetadataValue.text(dst),
            "schema": MetadataValue.text(_schema_meta(df)),
        }
    )

@asset(
    deps=[fact_movie_genre_ratings, fact_ratings, fact_tags, dim_movies],
    group_name="gold",
    kinds=GOLD_KINDS,
    pool="spark",
)
def mart_genre_performance(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    genre_ratings_src = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_movie_genre_ratings")
    ratings_src = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_ratings")
    tags_src = _to_s3a(f"s3://{bucket}/silver/intermediate/fact_tags")
    movies_src = _to_s3a(f"s3://{bucket}/silver/intermediate/dim_movies")
    dst = _to_s3a(f"s3://{bucket}/gold/marts/genre_performance")

    genre_ratings = spark.session.read.format("delta").load(genre_ratings_src)
    ratings = spark.session.read.format("delta").load(ratings_src)
    tags = spark.session.read.format("delta").load(tags_src)
    movies = spark.session.read.format("delta").load(movies_src).select("movie_id", "genres", "year")

    rating_metrics = genre_ratings.groupBy("genres").agg(
        F.countDistinct("movie_id").alias("unique_movies"),
        F.sum("rating_count").alias("total_ratings"),
        (F.sum(F.col("rating_count") * F.col("avg_rating")) / F.sum("rating_count")).alias("avg_rating"),
        F.min("min_rating").alias("min_rating"),
        F.max("max_rating").alias("max_rating"),
    )

    rating_detail_metrics = ratings.join(movies.select("movie_id", "genres"), on="movie_id", how="inner").groupBy("genres").agg(
        F.countDistinct("user_id").alias("unique_raters"),
        F.stddev("rating").alias("stddev_rating"),
        F.min("created_at").alias("first_rating_date"),
        F.max("created_at").alias("last_rating_date"),
    )

    tag_metrics = tags.join(movies.select("movie_id", "genres"), on="movie_id", how="inner").groupBy("genres").agg(
        F.count("*").alias("total_tags"),
        F.countDistinct("user_id").alias("unique_taggers"),
    )

    year_distribution = movies.groupBy("genres").agg(
        F.sum(F.when((F.col("year") >= 2020) & (F.col("year") < 2030), 1).otherwise(0)).alias("movies_2020s"),
        F.sum(F.when((F.col("year") >= 2010) & (F.col("year") < 2020), 1).otherwise(0)).alias("movies_2010s"),
        F.sum(F.when((F.col("year") >= 2000) & (F.col("year") < 2010), 1).otherwise(0)).alias("movies_2000s"),
        F.sum(F.when((F.col("year") >= 1990) & (F.col("year") < 2000), 1).otherwise(0)).alias("movies_1990s"),
        F.sum(F.when(F.col("year") < 1990, 1).otherwise(0)).alias("movies_before_1990"),
    )

    df = (
        rating_metrics.join(rating_detail_metrics, on="genres", how="inner")
        .join(tag_metrics, on="genres", how="left")
        .join(year_distribution, on="genres", how="left")
        .fillna({"total_tags": 0, "unique_taggers": 0})
        .withColumn(
            "rating_span_days",
            F.datediff(F.col("last_rating_date"), F.col("first_rating_date")),
        )
        .withColumn(
            "popularity_tier",
            F.when(F.col("total_ratings") >= 100000, F.lit("Highly Popular"))
            .when(F.col("total_ratings") >= 50000, F.lit("Very Popular"))
            .when(F.col("total_ratings") >= 10000, F.lit("Popular"))
            .when(F.col("total_ratings") >= 1000, F.lit("Moderately Popular"))
            .otherwise(F.lit("Niche")),
        )
        .withColumn(
            "quality_tier",
            F.when(F.col("avg_rating") >= 3.8, F.lit("Top Rated"))
            .when(F.col("avg_rating") >= 3.5, F.lit("Well Rated"))
            .when(F.col("avg_rating") >= 3.2, F.lit("Average Rated"))
            .otherwise(F.lit("Below Average")),
        )
        .withColumn(
            "avg_ratings_per_movie",
            F.when(F.col("unique_movies") > 0, F.col("total_ratings") / F.col("unique_movies")).otherwise(F.lit(0.0)),
        )
        .withColumn("refreshed_at", F.current_timestamp())
        .select(
            "genres",
            "unique_movies",
            "total_ratings",
            "unique_raters",
            "avg_rating",
            "min_rating",
            "max_rating",
            "stddev_rating",
            "first_rating_date",
            "last_rating_date",
            "rating_span_days",
            "total_tags",
            "unique_taggers",
            "movies_2020s",
            "movies_2010s",
            "movies_2000s",
            "movies_1990s",
            "movies_before_1990",
            "popularity_tier",
            "quality_tier",
            "avg_ratings_per_movie",
            "refreshed_at",
        )
    )

    row_count = _persist_full_refresh(df, dst)
    context.log.info("mart_genre_performance: %d rows -> %s", row_count, dst)
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "s3_path": MetadataValue.text(dst),
            "schema": MetadataValue.text(_schema_meta(df)),
        }
    )