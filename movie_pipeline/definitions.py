import os

from dagster import Definitions

from movie_pipeline.assets.bronze import bronze_links, bronze_movies, bronze_ratings, bronze_tags
from movie_pipeline.assets.silver_staging import stg_links, stg_movies, stg_ratings, stg_tags
from movie_pipeline.assets.silver_intermediate import dim_movies, dim_users, fact_ratings, fact_tags, fact_movie_genre_ratings
from movie_pipeline.checks.bronze_checks import (
    bronze_links_has_data,
    bronze_movies_has_data,
    bronze_ratings_has_data,
    bronze_tags_has_data,
)
from movie_pipeline.resources.s3_resource import S3Resource
from movie_pipeline.resources.spark_resource import SparkResource

defs = Definitions(
    assets=[
        bronze_movies, bronze_links, bronze_ratings, bronze_tags,
        stg_movies, stg_links, stg_ratings, stg_tags,
        dim_movies, dim_users, fact_ratings, fact_tags, fact_movie_genre_ratings,
    ],
    resources={
        "s3": S3Resource(
            aws_region=os.getenv("AWS_REGION", "ap-southeast-1"),
            bucket=os.getenv("S3_BUCKET"),
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
        bronze_movies_has_data,
        bronze_links_has_data,
        bronze_ratings_has_data,
        bronze_tags_has_data,
    ],
)
