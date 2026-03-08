from datetime import datetime, timezone

from dagster import MaterializeResult, MetadataValue, asset
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F

from movie_pipeline.assets.gold_marts import (
    GOLD_KINDS,
    mart_genre_performance,
    mart_movie_analytics,
    mart_user_behavior,
)
from movie_pipeline.resources.s3_resource import S3Resource
from movie_pipeline.resources.spark_resource import SparkResource

def _to_s3a(uri: str) -> str:
    return uri.replace("s3://", "s3a://", 1)

def _schema_meta(df: DataFrame) -> str:
    return "\n".join(f"  {field.name}: {field.dataType.simpleString()}" for field in df.schema.fields)

def _snapshot_change_condition(columns: list[str]) -> str:
    return " OR ".join(f"NOT (tgt.{column} <=> src.{column})" for column in columns)

def _snapshot_current_rows(
    spark: SparkResource,
    path: str,
    key_column: str,
    source: DataFrame,
    check_columns: list[str],
    effective_ts: datetime,
) -> tuple[int, int]:
    effective_ts_lit = F.lit(effective_ts).cast("timestamp")

    if not DeltaTable.isDeltaTable(spark.session, path):
        initial_df = (
            source.withColumn("valid_from", effective_ts_lit)
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )
        initial_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        total_rows = initial_df.count()
        return total_rows, total_rows

    delta_table = DeltaTable.forPath(spark.session, path)
    change_condition = _snapshot_change_condition(check_columns)

    (
        delta_table.alias("tgt")
        .merge(
            source.alias("src"),
            f"tgt.{key_column} = src.{key_column} AND tgt.is_current = true",
        )
        .whenMatchedUpdate(
            condition=change_condition,
            set={
                "valid_to": "CAST(current_timestamp() AS TIMESTAMP)",
                "is_current": "false",
            },
        )
        .execute()
    )

    active_snapshot = (
        spark.session.read.format("delta").load(path).filter(F.col("is_current") == True).select(key_column)
    )
    inserts = (
        source.alias("src")
        .join(active_snapshot.alias("cur"), on=key_column, how="left_anti")
        .withColumn("valid_from", effective_ts_lit)
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    inserted_rows = inserts.count()
    if inserted_rows > 0:
        inserts.write.format("delta").mode("append").save(path)

    snapshot_df = spark.session.read.format("delta").load(path)
    total_rows = snapshot_df.count()
    current_rows = snapshot_df.filter(F.col("is_current") == True).count()
    return total_rows, current_rows

def _snapshot_result(
    source: DataFrame,
    path: str,
    total_rows: int,
    current_rows: int,
) -> MaterializeResult:
    return MaterializeResult(
        metadata={
            "row_count": total_rows,
            "current_row_count": current_rows,
            "s3_path": MetadataValue.text(path),
            "schema": MetadataValue.text(_schema_meta(source)),
        }
    )

@asset(
    deps=[mart_movie_analytics],
    group_name="gold",
    kinds=GOLD_KINDS,
    pool="spark",
)
def snap_mart_movie_analytics(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    source_path = _to_s3a(f"s3://{bucket}/gold/marts/movie_analytics")
    snapshot_path = _to_s3a(f"s3://{bucket}/gold/snapshots/snap_mart_movie_analytics")
    source = spark.session.read.format("delta").load(source_path)
    total_rows, current_rows = _snapshot_current_rows(
        spark=spark,
        path=snapshot_path,
        key_column="movie_id",
        source=source,
        check_columns=["total_ratings", "avg_rating", "quality_tier"],
        effective_ts=datetime.now(timezone.utc),
    )
    context.log.info("snap_mart_movie_analytics: %d rows (%d current) -> %s", total_rows, current_rows, snapshot_path)
    return _snapshot_result(source, snapshot_path, total_rows, current_rows)

@asset(
    deps=[mart_user_behavior],
    group_name="gold",
    kinds=GOLD_KINDS,
    pool="spark",
)
def snap_mart_user_behavior(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    source_path = _to_s3a(f"s3://{bucket}/gold/marts/user_behavior")
    snapshot_path = _to_s3a(f"s3://{bucket}/gold/snapshots/snap_mart_user_behavior")
    source = spark.session.read.format("delta").load(source_path)
    total_rows, current_rows = _snapshot_current_rows(
        spark=spark,
        path=snapshot_path,
        key_column="user_id",
        source=source,
        check_columns=["total_ratings", "user_segment"],
        effective_ts=datetime.now(timezone.utc),
    )
    context.log.info("snap_mart_user_behavior: %d rows (%d current) -> %s", total_rows, current_rows, snapshot_path)
    return _snapshot_result(source, snapshot_path, total_rows, current_rows)

@asset(
    deps=[mart_genre_performance],
    group_name="gold",
    kinds=GOLD_KINDS,
    pool="spark",
)
def snap_mart_genre_performance(context, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    bucket = s3.require_bucket()
    source_path = _to_s3a(f"s3://{bucket}/gold/marts/genre_performance")
    snapshot_path = _to_s3a(f"s3://{bucket}/gold/snapshots/snap_mart_genre_performance")
    source = spark.session.read.format("delta").load(source_path)
    total_rows, current_rows = _snapshot_current_rows(
        spark=spark,
        path=snapshot_path,
        key_column="genres",
        source=source,
        check_columns=["total_ratings", "avg_rating", "popularity_tier"],
        effective_ts=datetime.now(timezone.utc),
    )
    context.log.info("snap_mart_genre_performance: %d rows (%d current) -> %s", total_rows, current_rows, snapshot_path)
    return _snapshot_result(source, snapshot_path, total_rows, current_rows)