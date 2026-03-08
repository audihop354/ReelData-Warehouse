from functools import cached_property

import boto3
from dagster import ConfigurableResource
from pydantic import Field

class SNSResource(ConfigurableResource):
    aws_region: str = Field(default="ap-southeast-1")
    topic_arn: str | None = Field(default=None)

    @cached_property
    def client(self):
        return boto3.client("sns", region_name=self.aws_region)

    def is_configured(self) -> bool:
        return bool(self.topic_arn)

    def publish(self, subject: str, message: str) -> dict | None:
        if not self.topic_arn:
            return None
        return self.client.publish(TopicArn=self.topic_arn, Subject=subject, Message=message)