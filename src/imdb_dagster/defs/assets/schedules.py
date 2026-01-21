import dagster as dg
from . import jobs

# There are no schedules defined currently, as the asset jobs are triggered by sensors and automation conditions.
# However, if you want to define schedules, you can uncomment and modify the example below.
# output_job_schedule = dg.ScheduleDefinition(
#     name="output_job_schedule",
#     job=output_job,
#     cron_schedule="*/15 * * * *",  # Every 15 minutes
# )