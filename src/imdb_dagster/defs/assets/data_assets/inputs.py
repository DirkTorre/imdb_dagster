import dagster as dg
import pandas as pd

from src.imdb_dagster.defs.assets import constants
from . import raw_inputs
from .... import helpers

from dagster import MetadataValue, TableRecord, TableSchema, TableColumn


@dg.asset(
    deps=[raw_inputs.title_basics],
    group_name="inputs",
    description="Processed IMDB title_basics DataFrame",
)
def title_basics(context: dg.AssetExecutionContext):
    cols_to_use = [
        "tconst",
        "primaryTitle",
        "originalTitle",
        "startYear",
        "runtimeMinutes",
        "genres",
    ]
    dtypes = {"startYear": pd.Int32Dtype(), "runtimeMinutes": pd.Int32Dtype()}

    df = pd.read_csv(
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

    meta_data: dg.MetadataValue = helpers.get_table_schema(df)

    return dg.MaterializeResult(
        value=df,
        metadata={
            "dagster/column_schema": meta_data.schema,
            "first_10_rows": meta_data,
            "total_records": dg.MetadataValue.int(len(df)),
        },
    )


@dg.asset(
    deps=[raw_inputs.title_ratings],
    group_name="inputs",
    description="Processed IMDB title_ratings DataFrame",
)
def title_ratings(context: dg.AssetExecutionContext):
    dtypes = {"averageRating": pd.Float32Dtype(), "numVotes": pd.Int32Dtype()}

    df = pd.read_csv(
        constants.TITLE_RATINGS_FILE_PATH,
        sep="\t",
        quotechar="\t",
        low_memory=False,
        dtype_backend="pyarrow",
        index_col="tconst",
        dtype=dtypes,
        na_values="\\N",
    )

    meta_data: dg.MetadataValue = helpers.get_table_schema(df)

    return dg.MaterializeResult(
        value=df,
        metadata={
            "dagster/column_schema": meta_data.schema,
            "first_10_rows": meta_data,
            "total_records": dg.MetadataValue.int(len(df)),
        },
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
    df = pd.read_csv(
        constants.DATES_AND_SCORES_FILE_PATH,
        dtype=dtypes,
        index_col="tconst",
        parse_dates=["date"],
    )

    # Convert datetime to date to retain only the date component
    df["date"] = df["date"].dt.date

    date_count = df.date.isna().value_counts()
    enjoyment_count = df.enjoyment_score.isna().value_counts()
    quality_counts = df.quality_score.isna().value_counts()

    meta_data: dg.MetadataValue = helpers.get_table_schema(df)

    return dg.MaterializeResult(
        value=df,
        metadata={
            "dagster/column_schema": meta_data.schema,
            "first_10_rows": meta_data,
            "total_records": dg.MetadataValue.int(len(df)),
            "has_date": dg.MetadataValue.int(int(date_count[False])),
            "has_no_date": dg.MetadataValue.int(int(date_count[True])),
            "has_enjoyment_score": dg.MetadataValue.int(int(enjoyment_count[False])),
            "has_no_enjoyment_score": dg.MetadataValue.int(int(enjoyment_count[True])),
            "has_quality_score": dg.MetadataValue.int(int(quality_counts[False])),
            "has_no_quality_score": dg.MetadataValue.int(int(quality_counts[True])),
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
    df = pd.read_csv(constants.STATUS_FILE_PATH, dtype=dtypes, index_col="tconst")

    watched = int(df["watched"].value_counts()[True])
    unwatched = int(df["watched"].value_counts()[False])
    total = len(df)

    meta_data: dg.MetadataValue = helpers.get_table_schema(df)

    return dg.MaterializeResult(
        value=df,
        metadata={
            "dagster/column_schema": meta_data.schema,
            "first_10_rows": meta_data,
            "total_records": dg.MetadataValue.int(total),
            "watched": dg.MetadataValue.int(watched),
            "unwatched": dg.MetadataValue.int(unwatched),
        },
    )
