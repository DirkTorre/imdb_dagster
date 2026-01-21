import dagster as dg
from .data_assets import inputs, outputs

"""
    "D" - only materializes D
    "*D" - materializes D and all upstreams (A, B, C, D)
    "+D" - materializes D and its direct upstream (C, D)
    "++D" - materializes D and two levels of upstreams (B, C, D)
"""

# runs when title_basics is out of date
title_basics_job = dg.define_asset_job(
    name="title_basics_job",
    selection=["*title_basics"]
)

# runs when title_ratings is out of date
title_ratings_job = dg.define_asset_job(
    name="title_ratings_job",
    selection=["*title_ratings"]
)

# runs when watched_dates_and_scores is updated
watched_dates_and_scores_job = dg.define_asset_job(
    name="watched_dates_and_scores_job",
    selection=["watched_dates_and_scores*"]
)

# runs when watch_status is updated
watch_status_job = dg.define_asset_job(
    name="watch_status_job", selection=["watch_status*"]
)