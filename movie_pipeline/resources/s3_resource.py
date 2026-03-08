from functools import cached_property

import boto3
from dagster import ConfigurableResource
from pydantic import Field

class S3Resource(ConfigurableResource):
    aws_region: str = Field(default="ap-southeast-1")
    bucket: str | None = Field(default=None)

    @cached_property
    def client(self):
        return boto3.client("s3", region_name=self.aws_region)

    def require_bucket(self) -> str:
        if not self.bucket:
            raise ValueError("S3 bucket is not configured.")
        return self.bucket

    def upload_file(self, source_path: str, object_key: str) -> str:
        bucket = self.require_bucket()
        self.client.upload_file(source_path, bucket, object_key)
        return self.object_uri(object_key)

    def object_uri(self, object_key: str) -> str:
        return f"s3://{self.require_bucket()}/{object_key}"
