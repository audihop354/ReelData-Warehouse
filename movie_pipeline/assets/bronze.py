from pathlib import Path
from dagster import MaterializeResult, MetadataValue, asset
from movie_pipeline.resources.s3_resource import S3Resource

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
BRONZE_KINDS = {"s3", "csv"}

def _count_csv_rows(file_path: Path) -> int:
    with file_path.open("r", encoding="utf-8") as csv_file:
        return max(sum(1 for _ in csv_file) - 1, 0)

def _materialize_bronze_file(context, s3: S3Resource, file_name: str, dataset_name: str) -> MaterializeResult:
    source_path = DATA_DIR / file_name
    if not source_path.exists():
        raise FileNotFoundError(f"Expected source file at {source_path}")
    row_count = _count_csv_rows(source_path)
    file_size_bytes = source_path.stat().st_size
    object_key = f"bronze/{dataset_name}/{file_name}"
    s3_uri = s3.upload_file(str(source_path), object_key)
    context.log.info("Uploaded %s to %s", file_name, s3_uri)
    return MaterializeResult(
        metadata={
            "source_path": MetadataValue.path(str(source_path)),
            "row_count": row_count,
            "file_size_bytes": file_size_bytes,
            "object_key": object_key,
            "s3_uri": s3_uri,
            "uploaded_to_s3": True,
        }
    )

@asset(group_name="bronze", kinds=BRONZE_KINDS)
def bronze_movies(context, s3: S3Resource) -> MaterializeResult:
    return _materialize_bronze_file(context=context, s3=s3, file_name="movies.csv", dataset_name="movies")

@asset(group_name="bronze", kinds=BRONZE_KINDS)
def bronze_links(context, s3: S3Resource) -> MaterializeResult:
    return _materialize_bronze_file(context=context, s3=s3, file_name="links.csv", dataset_name="links")

@asset(group_name="bronze", kinds=BRONZE_KINDS)
def bronze_ratings(context, s3: S3Resource) -> MaterializeResult:
    return _materialize_bronze_file(context=context, s3=s3, file_name="ratings.csv", dataset_name="ratings")

@asset(group_name="bronze", kinds=BRONZE_KINDS)
def bronze_tags(context, s3: S3Resource) -> MaterializeResult:
    return _materialize_bronze_file(context=context, s3=s3, file_name="tags.csv", dataset_name="tags")