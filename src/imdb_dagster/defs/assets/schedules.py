import dagster as dg

from . import jobs

watch_list_excel_schedule = dg.ScheduleDefinition(
    name="watch_list_excel_schedule",
    job=jobs.watch_list_excel_job,
    cron_schedule="*/5 * * * *",  # Every 5 minutes
)