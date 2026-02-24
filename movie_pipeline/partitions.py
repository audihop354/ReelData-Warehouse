from dagster import TimeWindowPartitionsDefinition


yearly_partitions = TimeWindowPartitionsDefinition(
    start="1995",
    end="2024",
    cron_schedule="@yearly",
    fmt="%Y",
)
