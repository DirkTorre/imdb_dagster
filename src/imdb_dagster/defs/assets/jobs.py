import dagster as dg
from .data_assets import inputs, outputs

"""
    "D" - only materializes D
    "*D" - materializes D and all upstreams (A, B, C, D)
    "+D" - materializes D and its direct upstream (C, D)
    "++D" - materializes D and two levels of upstreams (B, C, D)
"""


# je kan ook anders denken, als een input veranderd, genereer dan alles upstream daarvan

# Define a job that includes the output assets


# deze job moet runnen als de download sensors af gaan.
title_basics_job = dg.define_asset_job(
    name="title_basics_job",
    selection=["*title_basics"]
)

title_ratings_job = dg.define_asset_job(
    name="title_ratings_job",
    selection=["*title_ratings"]
)


watched_dates_and_scores_job = dg.define_asset_job(
    name="watched_dates_and_scores_job",
    selection=["watched_dates_and_scores*"]
)


watch_status_job = dg.define_asset_job(
    name="watch_status_job", selection=["watch_status*"]
)