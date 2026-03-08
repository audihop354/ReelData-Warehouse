import os

from dagster import AssetSelection, Definitions, define_asset_job

from movie_pipeline.assets.bronze import bronze_links, bronze_movies, bronze_ratings, bronze_tags
from movie_pipeline.assets.silver_staging import stg_links, stg_movies, stg_ratings, stg_tags
from movie_pipeline.assets.silver_intermediate import (
    dim_movies,
    dim_users,
    fact_movie_genre_ratings,
    fact_ratings,
    fact_tags,
)
from movie_pipeline.assets.gold_marts import (
    mart_genre_performance,
    mart_movie_analytics,
    mart_user_behavior,
)
from movie_pipeline.assets.gold_snapshots import (
    snap_mart_genre_performance,
    snap_mart_movie_analytics,
    snap_mart_user_behavior,
)
from movie_pipeline.assets.glue_catalog import register_gold_glue_catalog
from movie_pipeline.checks.silver_gold_checks import (
    dim_movies_genre_count_consistent,
    fact_movie_genre_ratings_preserves_exploded_rating_totals,
    fact_ratings_has_unique_business_key,
    fact_ratings_has_valid_movie_keys,
    fact_tags_has_valid_user_and_movie_keys,
    mart_genre_performance_has_expected_genre_set,
    mart_genre_performance_matches_distinct_genres,
    mart_movie_analytics_matches_distinct_movies,
    mart_movie_analytics_preserves_rating_totals,
    mart_user_behavior_matches_distinct_users,
)
from movie_pipeline.hooks.sns_hook import sns_failure_hook, sns_success_hook
from movie_pipeline.resources.glue_resource import GlueResource
from movie_pipeline.resources.s3_resource import S3Resource
from movie_pipeline.resources.sns_resource import SNSResource
from movie_pipeline.resources.spark_resource import SparkResource

all_assets_job = define_asset_job(
    name="all_assets_job",
    selection=AssetSelection.all(),
    hooks={sns_success_hook, sns_failure_hook},
)
defs = Definitions(
    assets=[
        bronze_movies, bronze_links, bronze_ratings, bronze_tags,
        stg_movies, stg_links, stg_ratings, stg_tags,
        dim_movies, dim_users, fact_ratings, fact_tags, fact_movie_genre_ratings,
        mart_movie_analytics, mart_user_behavior, mart_genre_performance,
        snap_mart_movie_analytics, snap_mart_user_behavior, snap_mart_genre_performance,
        register_gold_glue_catalog,
    ],
    resources={
        "s3": S3Resource(
            aws_region=os.getenv("AWS_REGION", "ap-southeast-1"),
            bucket=os.getenv("S3_BUCKET"),
        ),
        "glue": GlueResource(
            aws_region=os.getenv("AWS_REGION", "ap-southeast-1"),
            database=os.getenv("GLUE_DATABASE"),
        ),
        "sns": SNSResource(
            aws_region=os.getenv("SNS_REGION", os.getenv("AWS_REGION", "ap-southeast-1")),
            topic_arn=os.getenv("SNS_TOPIC_ARN"),
        ),
        "spark": SparkResource(
            app_name="movie-analysis",
            spark_master=os.getenv("SPARK_MASTER", "local[*]"),
            aws_region=os.getenv("AWS_REGION", "ap-southeast-1"),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "4g"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
        ),
    },
    asset_checks=[
        dim_movies_genre_count_consistent,
        fact_ratings_has_unique_business_key,
        fact_ratings_has_valid_movie_keys,
        fact_tags_has_valid_user_and_movie_keys,
        fact_movie_genre_ratings_preserves_exploded_rating_totals,
        mart_movie_analytics_matches_distinct_movies,
        mart_movie_analytics_preserves_rating_totals,
        mart_user_behavior_matches_distinct_users,
        mart_genre_performance_matches_distinct_genres,
        mart_genre_performance_has_expected_genre_set,
    ],
    jobs=[all_assets_job],
)
