from functools import cached_property

from dagster import ConfigurableResource
from delta import configure_spark_with_delta_pip
from pydantic import Field
from pyspark.sql import SparkSession


class SparkResource(ConfigurableResource):
    app_name: str = Field(default="movie-analysis")
    spark_master: str = Field(default="local[*]")
    aws_region: str = Field(default="ap-southeast-1")
    driver_memory: str = Field(default="4g")
    executor_memory: str = Field(default="4g")

    @cached_property
    def session(self) -> SparkSession:
        builder = (
            SparkSession.builder.appName(self.app_name)
            .master(self.spark_master)
            .config("spark.driver.memory", self.driver_memory)
            .config("spark.executor.memory", self.executor_memory)
            .config("spark.ui.enabled", "true")
            .config("spark.ui.port", "4040")
            .config("spark.ui.bindAddress", "0.0.0.0")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .config("spark.hadoop.fs.s3a.endpoint.region", self.aws_region)
            .config("spark.hadoop.fs.s3a.path.style.access", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.delta.logStore.s3.impl", "io.delta.storage.S3SingleDriverLogStore")
            .config("spark.delta.logStore.s3a.impl", "io.delta.storage.S3SingleDriverLogStore")
        )
        spark = configure_spark_with_delta_pip(
            builder,
            extra_packages=[
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ],
        ).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def stop(self) -> None:
        if "session" in self.__dict__:
            self.session.stop()