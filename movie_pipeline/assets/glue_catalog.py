from dagster import MaterializeResult, MetadataValue, asset

from movie_pipeline.assets.gold_marts import GOLD_KINDS, mart_genre_performance, mart_movie_analytics, mart_user_behavior
from movie_pipeline.assets.gold_snapshots import (
    snap_mart_genre_performance,
    snap_mart_movie_analytics,
    snap_mart_user_behavior,
)
from movie_pipeline.resources.glue_resource import GlueResource
from movie_pipeline.resources.s3_resource import S3Resource
from movie_pipeline.resources.spark_resource import SparkResource

def _to_s3a(uri: str) -> str:
    return uri.replace("s3://", "s3a://", 1)


@asset(
    deps=[
        mart_movie_analytics,
        mart_user_behavior,
        mart_genre_performance,
        snap_mart_movie_analytics,
        snap_mart_user_behavior,
        snap_mart_genre_performance,
    ],
    group_name="gold",
    kinds=GOLD_KINDS | {"glue"},
    pool="spark",
)
def register_gold_glue_catalog(
    context,
    spark: SparkResource,
    s3: S3Resource,
    glue: GlueResource,
) -> MaterializeResult:
    bucket = s3.require_bucket()
    database = glue.ensure_database()

    tables = {
        "mart_movie_analytics": f"s3://{bucket}/gold/marts/movie_analytics",
        "mart_user_behavior": f"s3://{bucket}/gold/marts/user_behavior",
        "mart_genre_performance": f"s3://{bucket}/gold/marts/genre_performance",
        "snap_mart_movie_analytics": f"s3://{bucket}/gold/snapshots/snap_mart_movie_analytics",
        "snap_mart_user_behavior": f"s3://{bucket}/gold/snapshots/snap_mart_user_behavior",
        "snap_mart_genre_performance": f"s3://{bucket}/gold/snapshots/snap_mart_genre_performance",
    }

    registered_tables: list[str] = []
    for table_name, s3_path in tables.items():
        schema = spark.session.read.format("delta").load(_to_s3a(s3_path)).schema
        glue.upsert_delta_table(table_name=table_name, location=s3_path, schema=schema)
        registered_tables.append(table_name)
        context.log.info("Registered %s in Glue database %s", table_name, database)

    return MaterializeResult(
        metadata={
            "glue_database": MetadataValue.text(database),
            "registered_table_count": len(registered_tables),
            "registered_tables": MetadataValue.json(registered_tables),
        }
    )