import dagster as dg
import pandas as pd

from .inputs import (
    watched_dates_and_scores,
    watch_status,
    title_basics,
    title_ratings,
)
from src.imdb_dagster.defs.assets import constants
from .... import helpers


@dg.asset(
    description="list of all the unique indices in the status files",
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
            "first 5 items": dg.MetadataValue.text(str(str(needed_indices[:5]))),
            "total records": dg.MetadataValue.int(len(needed_indices)),
        },
    )


@dg.asset(
    description="the needed rows from title_basics",
    group_name="intermediates",
    deps=["title_basics", "indices"],
    metadata={
        "dagster/column_schema": helpers.get_table_schema(
            [
                "tconst",
                "primaryTitle",
                "originalTitle",
                "startYear",
                "runtimeMinutes",
                "boolean genre columns",
            ]
        )
    },
)
def needed_title_basics(title_basics=title_basics, indices=indices):
    # Get missing indices
    not_in_title_basics = indices.difference(title_basics.index).to_list()

    # Get subset with existing indices
    needed_title_basics = title_basics.loc[indices.intersection(title_basics.index)]

    # Process genres
    genre_exploded = needed_title_basics["genres"].str.split(",").explode()
    genre = pd.crosstab(genre_exploded.index, genre_exploded)
    genre = genre.add_prefix("genre_").astype(pd.BooleanDtype())

    title_basics_crosstab_genre = needed_title_basics.drop(columns="genres").join(genre)

    # return not_in_title_basics, title_basics # how do i return two variables?

    return dg.MaterializeResult(
        value=title_basics_crosstab_genre,
        metadata={
            "first 5 rows": helpers.pandas_table_to_dagster_preview(
                title_basics_crosstab_genre
            ),
            "total records": dg.MetadataValue.int(title_basics_crosstab_genre.shape[0]),
            "not_found": dg.MetadataValue.text(str(not_in_title_basics)),
        },
    )


@dg.asset(
    description="the needed rows from title_ratings",
    group_name="intermediates",
    deps=["title_ratings", "indices"],
    metadata={
        "dagster/column_schema": helpers.get_table_schema(
            ["tconst", "averageRating", "numVotes"]
        )
    },
)
def needed_title_ratings(title_ratings=title_ratings, indices=indices):
    # Get missing indices
    not_in_title_ratings = indices.difference(title_ratings.index).to_list()

    # Get subset with existing indices
    needed_title_ratings = title_ratings.loc[indices.intersection(title_ratings.index)]

    # return not_in_title_ratings, title_ratings # how do i return two variables?

    return dg.MaterializeResult(
        value=needed_title_ratings,
        metadata={
            "first 5 rows": helpers.pandas_table_to_dagster_preview(
                needed_title_ratings
            ),
            "total records": dg.MetadataValue.int(needed_title_ratings.shape[0]),
            "not_found": dg.MetadataValue.text(str(not_in_title_ratings)),
        },
    )


@dg.asset(
    description="The movie list with the watch status enriched with IMDb data",
    group_name="intermediates",
    deps=["watch_status", "needed_title_basics", "needed_title_ratings"],
    metadata={
        "dagster/column_schema": helpers.get_table_schema(
            [
                "tconst",
                "watched",
                "priority",
                "netflix",
                "prime",
                "originalTitle",
                "startYear",
                "runtimeMinutes",
                "boolean genre columns",
            ]
        )
    },
)
def my_movie_list(
    watch_status,
    needed_title_basics,
    needed_title_ratings,
):
    movie_list = (
        watch_status.join(needed_title_basics)
        .join(needed_title_ratings)
        .sort_values(
            ["watched", "priority", "averageRating"], ascending=[True, False, False]
        )
    )
    not_found = movie_list.index.difference(watch_status.index).to_list()

    return dg.MaterializeResult(
        value=movie_list,
        metadata={
            "first 5 rows": helpers.pandas_table_to_dagster_preview(movie_list),
            "total records": dg.MetadataValue.int(movie_list.shape[0]),
            "not_found": dg.MetadataValue.text(str(not_found)),
        },
    )


@dg.asset(
    description="My movie reviews enriched with IMDb data",
    group_name="intermediates",
    deps=["watched_dates_and_scores", "needed_title_basics", "needed_title_ratings"],
    metadata={
        "dagster/column_schema": helpers.get_table_schema(
            [
                "tconst",
                "date",
                "enjoyment_score",
                "quality_score",
                "primaryTitle",
                "originalTitle",
                "startYear",
            ]
        )
    },
)
def my_movie_reviews(
    watched_dates_and_scores,
    needed_title_basics,
):
    movie_reviews = watched_dates_and_scores.join(
        needed_title_basics[["primaryTitle", "originalTitle", "startYear"]]
    ).sort_values("date")
    not_found = movie_reviews.index.difference(watched_dates_and_scores.index).to_list()

    return dg.MaterializeResult(
        value=movie_reviews,
        metadata={
            "first 5 rows": helpers.pandas_table_to_dagster_preview(movie_reviews),
            "total records": dg.MetadataValue.int(movie_reviews.shape[0]),
            "not_found": dg.MetadataValue.text(str(not_found)),
        },
    )
