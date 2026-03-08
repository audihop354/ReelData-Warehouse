from functools import cached_property

import boto3
from botocore.exceptions import ClientError
from dagster import ConfigurableResource
from pydantic import Field
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
)

class GlueResource(ConfigurableResource):
    aws_region: str = Field(default="ap-southeast-1")
    database: str | None = Field(default=None)

    @cached_property
    def client(self):
        return boto3.client("glue", region_name=self.aws_region)

    def require_database(self) -> str:
        if not self.database:
            raise ValueError("Glue database is not configured.")
        return self.database

    def ensure_database(self) -> str:
        database = self.require_database()
        try:
            self.client.get_database(Name=database)
        except self.client.exceptions.EntityNotFoundException:
            self.client.create_database(DatabaseInput={"Name": database})
        return database

    def upsert_delta_table(self, table_name: str, location: str, schema: StructType) -> None:
        database = self.ensure_database()
        table_input = {
            "Name": table_name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "EXTERNAL": "TRUE",
                "classification": "delta",
                "table_type": "DELTA",
            },
            "StorageDescriptor": {
                "Columns": self._schema_to_glue_columns(schema),
                "Location": location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
            },
        }

        try:
            self.client.create_table(DatabaseName=database, TableInput=table_input)
        except self.client.exceptions.AlreadyExistsException:
            self.client.update_table(DatabaseName=database, TableInput=table_input)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "AlreadyExistsException":
                self.client.update_table(DatabaseName=database, TableInput=table_input)
            else:
                raise

    def _schema_to_glue_columns(self, schema: StructType) -> list[dict[str, str]]:
        return [{"Name": field.name, "Type": self._spark_type_to_glue_type(field.dataType)} for field in schema.fields]

    def _spark_type_to_glue_type(self, data_type: DataType) -> str:
        if isinstance(data_type, StringType):
            return "string"
        if isinstance(data_type, (IntegerType, ShortType)):
            return "int"
        if isinstance(data_type, LongType):
            return "bigint"
        if isinstance(data_type, (DoubleType, FloatType)):
            return "double"
        if isinstance(data_type, BooleanType):
            return "boolean"
        if isinstance(data_type, TimestampType):
            return "timestamp"
        if isinstance(data_type, DateType):
            return "date"
        if isinstance(data_type, DecimalType):
            return f"decimal({data_type.precision},{data_type.scale})"
        if isinstance(data_type, ArrayType):
            return f"array<{self._spark_type_to_glue_type(data_type.elementType)}>"
        if isinstance(data_type, MapType):
            return (
                f"map<{self._spark_type_to_glue_type(data_type.keyType)},"
                f"{self._spark_type_to_glue_type(data_type.valueType)}>"
            )
        if isinstance(data_type, StructType):
            fields = ",".join(
                f"{field.name}:{self._spark_type_to_glue_type(field.dataType)}" for field in data_type.fields
            )
            return f"struct<{fields}>"
        return "string"