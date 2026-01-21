import dagster as dg
import pandas as pd

from src.imdb_dagster.defs.assets import constants
from .data_assets.inputs import (
    title_basics,
    watched_dates_and_scores,
    watch_status,
)


# -------------------------------------------------------------------
# Check: watch_status has no duplicate tconst
# -------------------------------------------------------------------
@dg.asset_check(asset=watch_status, blocking=True)
def watch_status_has_no_duplicate_tconst() -> dg.AssetCheckResult:
    """Ensure the watch_status CSV contains no duplicate tconst values."""

    dtypes = {
        "tconst": pd.StringDtype(),
        "watched": pd.BooleanDtype(),
        "priority": pd.BooleanDtype(),
        "netflix": pd.BooleanDtype(),
        "prime": pd.BooleanDtype(),
    }

    df = pd.read_csv(
        constants.STATUS_FILE_PATH,
        dtype=dtypes,
        index_col="tconst",
    )

    duplicated_mask = df.index.duplicated()
    duplicates = df.index[duplicated_mask].tolist()

    return dg.AssetCheckResult(
        passed=len(duplicates) == 0,
        metadata={"duplicates": duplicates},
    )


# -------------------------------------------------------------------
# Factory: generic tconst existence check
# -------------------------------------------------------------------
def create_tconst_check(asset_name: str) -> dg.AssetCheckResult:
    """
    Creates a check ensuring that all tconst values in `asset`
    exist in title_basics.
    """

    @dg.asset_check(
        asset=asset_name,
        name=f"{asset_name}_tconst_exists_in_title_basics",
        additional_ins={"title_basics": dg.AssetIn("title_basics")},
        blocking=True,
    )
    def _check(context, asset_value, title_basics) -> dg.AssetCheckResult:
        # asset_value is the value of the asset being checked (comes from the decorator "asset")
        # title_basics comes from additional_ins
        # NOTE: the asset to be checked must always come first
        missing = asset_value.index.difference(title_basics.index).tolist()
        passed = len(missing) == 0

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "missing_tconst": missing,
            },
        )

    return _check


# Instantiate checks
watch_status_check: dg.AssetCheckResult = create_tconst_check("watch_status")
watched_dates_check: dg.AssetCheckResult = create_tconst_check(
    "watched_dates_and_scores"
)


# -------------------------------------------------------------------
# Check: watched_dates_and_scores tconst must exist in watch_status
# -------------------------------------------------------------------
@dg.asset_check(
    asset=watch_status,
    name="watched_dates_and_scores_tconst_in_watch_status",
    additional_ins={"watched_dates_and_scores": dg.AssetIn("watched_dates_and_scores")},
    blocking=True,
)
def tconsts_in_watch_status(
    context, watch_status: pd.DataFrame, watched_dates_and_scores: pd.DataFrame
) -> dg.AssetCheckResult:
    missing: list = watched_dates_and_scores.index.difference(
        watch_status.index
    ).tolist()
    passed = len(missing) == 0

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "missing_tconst": missing,
            "message": (
                "All tconst values in watched_dates_and_scores exist in watch_status"
                if passed
                else f"Missing tconst values: {missing}"
            ),
        },
    )


# -------------------------------------------------------------------
# Check: watched_dates_and_scores entries must be marked watched=True in watch_status
# -------------------------------------------------------------------
@dg.asset_check(
    asset=watch_status,
    name="watched_dates_and_scores_marked_as_watched",
    additional_ins={"watched_dates_and_scores": dg.AssetIn("watched_dates_and_scores")},
    blocking=True,
)
def watched_dates_marked_as_watched(
    context, watch_status: pd.DataFrame, watched_dates_and_scores: pd.DataFrame
) -> dg.AssetCheckResult:
    """
    Ensures that any movie with a watched date is marked watched=True in watch_status.
    """
    watched_dates_and_scores["watched"] = True

    # Align only overlapping indices
    overlap: pd.DataFrame = watched_dates_and_scores.join(
        watch_status, how="inner", lsuffix="_wd", rsuffix="_ws"
    )

    all_true_in_ws = bool(overlap["watched_ws"].all())
    mismatch: list = overlap[overlap["watched_ws"] == False].index.unique().to_list()

    return dg.AssetCheckResult(
        passed=all_true_in_ws,
        metadata={"message": f"not true in watch_status: {mismatch}"},
    )
