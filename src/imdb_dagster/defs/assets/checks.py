# contains circular dependency

import dagster as dg
import pandas as pd
import os
import time
from datetime import datetime

from src.imdb_dagster.defs.assets import constants
from .data_assets import raw_inputs, inputs
from .data_assets.inputs import title_basics, watched_dates_and_scores, watch_status


# from .data_assets.raw_inputs import watch_status, watched_dates_and_scores
# from .inputs import title_basics

            

@dg.asset_check(asset=inputs.watch_status, blocking=True)
def watch_status_has_no_duplicate_tconst():
    dtypes = {
        "tconst": pd.StringDtype(),
        "watched": pd.BooleanDtype(),
        "priority": pd.BooleanDtype(),
        "netflix": pd.BooleanDtype(),
        "prime": pd.BooleanDtype(),
    }

    status = pd.read_csv(constants.STATUS_FILE_PATH, dtype=dtypes, index_col="tconst")

    duplicated = status.index.duplicated()
    dups = []
    if duplicated.any():
        dups = list(status[duplicated].index)

    return dg.AssetCheckResult(
        passed=len(dups) == 0,
        metadata={
            "duplicate id's": dups,
        },
    )

# code work's even when title_basics isn't loaded yet when materializing date_scores or status
def create_tconst_check(asset_name: str):
    @dg.asset_check(
        asset=asset_name,
        name=f"{asset_name}_tconst_exists",
        additional_ins={"title_basics": dg.AssetIn("title_basics")},
        blocking=True
    )
    def tconst_exists(context, asset_value, title_basics):
        # asset_value is the value of the asset being checked (comes from the decorator "asset")
        # title_basics comes from additional_ins
        # NOTE: the asset to be checked must always come first
        
        not_exists = asset_value.index.difference(title_basics.index).to_list()
        all_exist = not bool(not_exists)
        
        if all_exist:
            passed = True
            message = f"all tconst's were found in title_basics for {asset_name}"
        else:
            passed = False
            message = f"some tconst's were not found in title_basics for {asset_name}: \
                {' '.join(not_exists)}"

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "message": message
            },
        )

    return tconst_exists


# Create the checks - only pass the asset name
watch_status_check = create_tconst_check("watch_status")
watched_date_check = create_tconst_check("watched_dates_and_scores")



@dg.asset_check(
    asset=watch_status,
    name="tconsts_from_watched_dates_and_scores_are_in_watch_status",
    additional_ins={"watched_dates_and_scores": dg.AssetIn("watched_dates_and_scores")},
    blocking=True
)
def tconst_exists(context, watch_status, watched_dates_and_scores):
    not_in_watch_status = watched_dates_and_scores.index.difference(
        watch_status.index
    ).to_list()
    passed_value = not not_in_watch_status  # true when empty

    message = ""
    if passed_value:
        message = "all tconst's were found in watch_status"
    else:
        message = f"not found in watch_status, please add: {not_in_watch_status}"

    return dg.AssetCheckResult(passed=passed_value, metadata={"message": message})



@dg.asset_check(
    asset=watch_status,
    name=f"watched_dates_and_scores_marked_as_watched_in_watch_status",
    additional_ins={"watched_dates_and_scores": dg.AssetIn("watched_dates_and_scores")},
    blocking=True
)
def watch_status_updated_with_wdas(context, watch_status, watched_dates_and_scores):
    watched_dates_and_scores["watched"] = True

    # Align only overlapping indices
    overlap = watched_dates_and_scores.join(
        watch_status, how="inner", lsuffix="_wd", rsuffix="_ws"
    )

    all_true_in_ws = bool(overlap["watched_ws"].all())
    mismatch = overlap[overlap["watched_ws"] == False].index.unique().to_list()

    return dg.AssetCheckResult(
        passed=all_true_in_ws,
        metadata={"message": f"not true in watch_status: {mismatch}"},
    )
