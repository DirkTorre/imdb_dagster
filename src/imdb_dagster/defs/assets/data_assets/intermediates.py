import dagster as dg
import pandas as pd

from .inputs import (
    watched_dates_and_scores,
    watch_status,
    title_basics,
    title_ratings,
)
from .... import helpers


@dg.asset(
    description="Union of all unique indices from watch_status and watched_dates_and_scores",
    group_name="intermediates",
    deps=["watched_dates_and_scores", "watch_status"],
)
def indices(
    watched_dates_and_scores=watched_dates_and_scores, watch_status=watch_status
):
    needed_indices = watched_dates_and_scores.index.union(watch_status.index)

    return dg.MaterializeResult(
        value=needed_indices,
        metadata={
            "first 5 items": dg.MetadataValue.text(str(needed_indices[:5])),
            "total records": dg.MetadataValue.int(len(needed_indices)),
        },
    )


@dg.asset(
    description="Subset of title_basics containing only needed indices",
    group_name="intermediates",
    deps=["title_basics", "indices"],
)
def needed_title_basics(title_basics=title_basics, indices=indices):
    missing = indices.difference(title_basics.index)
    present = indices.intersection(title_basics.index)
    df = title_basics.loc[present]

    # Expand genres into boolean columns
    genre_exploded = df["genres"].str.split(",").explode()
    genre = (
        pd.crosstab(genre_exploded.index, genre_exploded)
        .add_prefix("genre_")
        .astype(pd.BooleanDtype())
    )

    df_final = df.drop(columns="genres").join(genre)

    meta_data: dg.MetadataValue = helpers.get_table_schema(df_final)

    return dg.MaterializeResult(
        value=df_final,
        metadata={
            "dagster/column_schema": meta_data.schema,
            "first_10_rows": meta_data,
            "total records": dg.MetadataValue.int(len(df_final)),
            "not_found": dg.MetadataValue.text(str(missing.to_list())),
        },
    )


@dg.asset(
    description="Subset of title_ratings containing only needed indices",
    group_name="intermediates",
    deps=["title_ratings", "indices"],
)
def needed_title_ratings(title_ratings=title_ratings, indices=indices):
    missing = indices.difference(title_ratings.index)
    present = indices.intersection(title_ratings.index)
    df = title_ratings.loc[present]

    meta_data: dg.MetadataValue = helpers.get_table_schema(df)

    return dg.MaterializeResult(
        value=df,
        metadata={
            "dagster/column_schema": meta_data.schema,
            "first_10_rows": meta_data,
            "total records": dg.MetadataValue.int(len(df)),
            "not_found": dg.MetadataValue.text(str(missing.to_list())),
        },
    )


@dg.asset(
    description="Watch status enriched with IMDb basics and ratings",
    group_name="intermediates",
    deps=["watch_status", "needed_title_basics", "needed_title_ratings"],
)
def my_movie_list(
    watch_status,
    needed_title_basics,
    needed_title_ratings,
):
    df = (
        watch_status.join(needed_title_ratings, how="left")
        .join(needed_title_basics, how="left")
        .sort_values(
            ["watched", "priority", "averageRating"], ascending=[True, False, False]
        )
    )

    missing = df.index.difference(watch_status.index)

    meta_data: dg.MetadataValue = helpers.get_table_schema(df)

    return dg.MaterializeResult(
        value=df,
        metadata={
            "dagster/column_schema": meta_data.schema,
            "first_10_rows": meta_data,
            "not_found": dg.MetadataValue.text(str(missing.to_list())),
        },
    )


@dg.asset(
    description="My movie reviews enriched with IMDb data",
    group_name="intermediates",
    deps=["watched_dates_and_scores", "needed_title_basics", "needed_title_ratings"],
)
def my_movie_reviews(
    watched_dates_and_scores,
    needed_title_basics,
):
    df = watched_dates_and_scores.join(
        needed_title_basics[["primaryTitle", "originalTitle", "startYear"]], how="left"
    ).sort_values("date")

    missing = df.index.difference(watched_dates_and_scores.index)

    meta_data: dg.MetadataValue = helpers.get_table_schema(df)

    return dg.MaterializeResult(
        value=df,
        metadata={
            "dagster/column_schema": meta_data.schema,
            "first_10_rows": meta_data,
            "total_records": dg.MetadataValue.int(len(df)),
            "not_found": dg.MetadataValue.text(str(missing.to_list())),
        },
    )
