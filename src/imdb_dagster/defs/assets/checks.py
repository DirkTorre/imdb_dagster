import dagster as dg
import pandas as pd

from src.imdb_dagster.defs.assets import constants
from .transformation import watch_status


@dg.asset_check(asset=watch_status)
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


# TODO: kijken of de tconsts in watch_status en watched_dates_and_scores wel bestaan
# TODO: kijken of er geen duplicaten zijn in watched_dates_and_scores
# TODO: kijken of alle films in watched_dates_and_scores wel zijn gemakreerd als gekeken in watch_status
