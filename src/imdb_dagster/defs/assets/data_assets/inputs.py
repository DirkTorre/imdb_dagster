import dagster as dg
import pandas as pd
import requests
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe
import os
import time
from datetime import datetime, timedelta

from src.imdb_dagster.defs.assets import constants
from . import raw_inputs
from .... import helpers


@dg.asset(
    deps=[raw_inputs.title_basics],
    group_name="inputs",
    description="Processed IMDB title_basics DataFrame",
    metadata={
        "dagster/column_schema": helpers.get_table_schema(
            [
                "tconst",
                "primaryTitle",
                "originalTitle",
                "startYear",
                "runtimeMinutes",
                "genres",
            ]
        )
    },
)
def title_basics(context: dg.AssetExecutionContext):
    """Load and process the title_basics file into a DataFrame."""

    cols_to_use = [
        "tconst",
        "primaryTitle",
        "originalTitle",
        "startYear",
        "runtimeMinutes",
        "genres",
    ]
    dtypes = {"startYear": pd.Int32Dtype(), "runtimeMinutes": pd.Int32Dtype()}

    title_basics_df = pd.read_csv(
        constants.TITLE_BASICS_FILE_PATH,
        sep="\t",
        quotechar="\t",
        low_memory=False,
        dtype_backend="pyarrow",
        usecols=cols_to_use,
        index_col="tconst",
        dtype=dtypes,
        na_values="\\N",
    )

    return dg.MaterializeResult(
        value=title_basics_df,
        metadata={
            "first 5 rows": helpers.pandas_table_to_dagster_preview(title_basics_df),
            "total_records": dg.MetadataValue.int(title_basics_df.shape[0]),
        },
    )


@dg.asset(
    deps=[raw_inputs.title_ratings],
    group_name="inputs",
    description="Processed IMDB title_ratings DataFrame",
    metadata={
        "dagster/column_schema": helpers.get_table_schema(
            ["tconst", "averageRating", "numVotes"]
        )
    },
)
def title_ratings(context: dg.AssetExecutionContext):
    """Load and process the title_basics file into a DataFrame."""

    dtypes = {"averageRating": pd.Float32Dtype(), "numVotes": pd.Int32Dtype()}

    title_ratings_df = pd.read_csv(
        constants.TITLE_RATINGS_FILE_PATH,
        sep="\t",
        quotechar="\t",
        low_memory=False,
        dtype_backend="pyarrow",
        index_col="tconst",
        dtype=dtypes,
        na_values="\\N",
    )

    return dg.MaterializeResult(
        value=title_ratings_df,
        metadata={
            "first 5 rows": helpers.pandas_table_to_dagster_preview(title_ratings_df),
            "total_records": dg.MetadataValue.int(title_ratings_df.shape[0]),
        },
    )


@dg.asset(
    group_name="inputs",
    description="The dates movies have been watched and scores I gave them",
    metadata={
        "dagster/column_schema": helpers.get_table_schema(
            [
                "tconst",
                "date",
                "enjoyment_score",
                "quality_score",
                "boolean genre columns",
            ]
        )
    },
)
def watched_dates_and_scores():
    dtypes = {
        "tconst": pd.StringDtype(),
        "enjoyment_score": pd.Float32Dtype(),
        "quality_score": pd.Float32Dtype(),
    }
    date_scores = pd.read_csv(
        constants.DATES_AND_SCORES_FILE_PATH,
        dtype=dtypes,
        index_col="tconst",
        parse_dates=["date"],
    )

    # Convert datetime to date to retain only the date component
    date_scores["date"] = date_scores["date"].dt.date

    date_count = date_scores.date.isna().value_counts()
    enjoyment_count = date_scores.enjoyment_score.isna().value_counts()
    quality_counts = date_scores.quality_score.isna().value_counts()

    return dg.MaterializeResult(
        value=date_scores,
        metadata={
            "first 5 rows": helpers.pandas_table_to_dagster_preview(date_scores),
            "total records": dg.MetadataValue.int(len(date_scores)),
            "has date": dg.MetadataValue.int(int(date_count[False])),
            "has no date": dg.MetadataValue.int(int(date_count[True])),
            "has enjoyment score": dg.MetadataValue.int(int(enjoyment_count[False])),
            "has no enjoyment score": dg.MetadataValue.int(int(enjoyment_count[True])),
            "has quality score": dg.MetadataValue.int(int(quality_counts[False])),
            "has no quality score": dg.MetadataValue.int(int(quality_counts[True])),
        },
    )


@dg.asset(
    group_name="inputs",
    description="My movie list with info about if they have been watched and where they can be viewed",
    metadata={
        "dagster/column_schema": helpers.get_table_schema(
            ["tconst", "watched", "priority", "netflix", "prime"]
        )
    },
)
def watch_status():
    dtypes = {
        "tconst": pd.StringDtype(),
        "watched": pd.BooleanDtype(),
        "priority": pd.BooleanDtype(),
        "netflix": pd.BooleanDtype(),
        "prime": pd.BooleanDtype(),
    }
    status = pd.read_csv(constants.STATUS_FILE_PATH, dtype=dtypes, index_col="tconst")

    watched = int(status["watched"].value_counts()[True])
    unwatched = int(status["watched"].value_counts()[False])
    total = len(status)

    return dg.MaterializeResult(
        value=status,
        metadata={
            "first 5 rows": helpers.pandas_table_to_dagster_preview(status),
            "total records": dg.MetadataValue.int(total),
            "watched": dg.MetadataValue.int(watched),
            "unwatched": dg.MetadataValue.int(unwatched),
        },
    )
