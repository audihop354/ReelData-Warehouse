from dagster import HookContext, failure_hook, success_hook

from movie_pipeline.resources.sns_resource import SNSResource

def _publish_if_configured(context: HookContext, status: str) -> None:
    sns: SNSResource = context.resources.sns
    if not sns.is_configured():
        return

    message = (
        f"Job: {context.job_name}\n"
        f"Op: {context.op.name if context.op else 'unknown'}\n"
        f"Run ID: {context.run_id}\n"
        f"Status: {status}"
    )
    sns.publish(subject=f"Dagster {status}: {context.job_name}", message=message)

@success_hook(required_resource_keys={"sns"})
def sns_success_hook(context: HookContext) -> None:
    _publish_if_configured(context, "SUCCESS")

@failure_hook(required_resource_keys={"sns"})
def sns_failure_hook(context: HookContext) -> None:
    _publish_if_configured(context, "FAILURE")