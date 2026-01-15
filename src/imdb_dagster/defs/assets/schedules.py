import dagster as dg
from . import jobs

# hoeft geen schedule the hebben
# output_job_schedule = dg.ScheduleDefinition(
#     name="output_job_schedule",
#     job=output_job,
#     cron_schedule="*/15 * * * *",  # Every 15 minutes
# )