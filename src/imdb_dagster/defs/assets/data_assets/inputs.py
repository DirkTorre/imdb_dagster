import dagster as dg
import pandas as pd
import requests
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe
import os
import time
from datetime import datetime, timedelta

from src.imdb_dagster.defs.assets import constants

from . import raw_inputs


# @dg.asset(group_name="inputs", description="imdb file title_basics.csv")
# def title_basics():
#     file_exists = os.path.exists(constants.TITLE_BASICS_FILE_PATH)
#     mod_time = os.path.getmtime(constants.TITLE_BASICS_FILE_PATH)
#     current_time = time.time()
#     time_passed = current_time - mod_time

#     cols_to_use = [
#         "tconst",
#         "primaryTitle",
#         "originalTitle",
#         "startYear",
#         "runtimeMinutes",
#         "genres",
#     ]
#     dtypes = {"startYear": pd.Int32Dtype(), "runtimeMinutes": pd.Int32Dtype()}
#     fresh_download = False

#     # download if file not there
#     if not file_exists or time_passed > 3600 * 24:
#         response = requests.get("https://datasets.imdbws.com/title.basics.tsv.gz")
#         with open(constants.TITLE_BASICS_FILE_PATH, "wb") as output_file:
#             output_file.write(response.content)
#         fresh_download = True

#     # load file
#     title_basics = pd.read_csv(
#         constants.TITLE_BASICS_FILE_PATH,
#         sep="\t",
#         quotechar="\t",
#         low_memory=False,
#         dtype_backend="pyarrow",
#         usecols=cols_to_use,
#         index_col="tconst",
#         dtype=dtypes,
#         na_values="\\N",
#     )

#     return dg.MaterializeResult(
#         value=title_basics,
#         metadata={
#             "total records": dg.MetadataValue.int(title_basics.shape[0]),
#             "fresh download": dg.MetadataValue.bool(fresh_download),
#         },
#     )


# @dg.asset(group_name="inputs", description="imdb file title_ratings.csv")
# def title_ratings():
#     file_exists = os.path.exists(constants.TITLE_RATINGS_FILE_PATH)
#     mod_time = os.path.getmtime(constants.TITLE_RATINGS_FILE_PATH)
#     current_time = time.time()
#     time_passed = current_time - mod_time

#     dtypes = {"averageRating": pd.Float32Dtype(), "numVotes": pd.Int32Dtype()}
#     fresh_download = False

#     # download if file not there
#     if not file_exists or time_passed > 3600 * 24:
#         response = requests.get("https://datasets.imdbws.com/title.ratings.tsv.gz")
#         with open(constants.TITLE_RATINGS_FILE_PATH, "wb") as output_file:
#             output_file.write(response.content)
#         fresh_download = True

#     # load file
#     title_ratings = pd.read_csv(
#         constants.TITLE_RATINGS_FILE_PATH,
#         sep="\t",
#         quotechar="\t",
#         low_memory=False,
#         dtype_backend="pyarrow",
#         index_col="tconst",
#         dtype=dtypes,
#         na_values="\\N",
#     )

#     return dg.MaterializeResult(
#         value=title_ratings,
#         metadata={
#             "total records": dg.MetadataValue.int(title_ratings.shape[0]),
#             "fresh download": dg.MetadataValue.bool(fresh_download),
#         },
#     )




# Asset for the processed DataFrame
@dg.asset(
    deps=[raw_inputs.title_ratings],
    group_name="inputs",
    description="Processed IMDB title_ratings DataFrame",
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
            "total_records": dg.MetadataValue.int(title_ratings_df.shape[0])
        }
    )


# Asset for the processed DataFrame
@dg.asset(
    deps=[raw_inputs.title_basics],
    group_name="inputs",
    description="Processed IMDB title_basics DataFrame",
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
            "total_records": dg.MetadataValue.int(title_basics_df.shape[0])
        }
    )


@dg.asset(
    group_name="inputs",
    description="The dates movies have been watched and scores I gave them",
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
            "total records": dg.MetadataValue.int(total),
            "watched": dg.MetadataValue.int(watched),
            "unwatched": dg.MetadataValue.int(unwatched),
            "tabular_data": dg.MetadataValue.md(status.head().to_markdown()),
            "table_schema": create_table_schema_metadata_from_dataframe(
                status
            ),  # does not include index...
        },
    )
