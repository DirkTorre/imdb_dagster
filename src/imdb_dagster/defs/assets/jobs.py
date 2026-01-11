# download title_ratings
# download title_basics
# create excel file (and visualisation)

# https://docs.dagster.io/guides/build/jobs/asset-jobs

# seeparte downloading files from laoding files??

from .data_assets import inputs 
# from .data_assets import products

import dagster as dg

# from .data_assets.raw_inputs import title_ratings_raw, title_basics_raw
# from .data_assets.inputs import title_ratings, title_basics


# imdb_refresh_job = dg.define_asset_job(
#     name="imdb_refresh_job",
#     selection=[title_basics_raw, title_basics]
# )

# # Schedule to run daily at 2 AM
# @dg.schedule(
#     job=imdb_refresh_job,
#     cron_schedule="0 2 * * *"  # Daily at 2 AM
# )
# def daily_imdb_refresh_schedule(context):
#     """Refresh IMDB data daily."""
#     return dg.RunRequest(
#         run_key=context.scheduled_execution_time.strftime("%Y-%m-%d")
#     )

watched_dates_and_scores_job = dg.define_asset_job(
    name="watched_dates_and_scores_job",
    selection=[inputs.watched_dates_and_scores]
)


watch_status_job = dg.define_asset_job(
    name="watch_status_job",
    selection=[inputs.watch_status]
)

# # materialized assets that are already synced
# watch_list_excel_job = dg.define_asset_job(
#     name="watch_list_excel_job",
#     selection=dg.AssetSelection.keys("watch_list_excel").upstream()
# )