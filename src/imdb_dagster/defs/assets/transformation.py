import dagster as dg
import pandas as pd
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe

from .ingestion import watched_dates_and_scores, watchlist, title_basics, title_ratings

@dg.asset(
    description="all the unique indices in the status files",
    group_name="intermediates",
    deps=["watched_dates_and_scores", "watchlist"],
)
def indices(
    watched_dates_and_scores=watched_dates_and_scores,
    watchlist=watchlist):

    needed_indices = watched_dates_and_scores.index.union(watchlist.index)

    return dg.MaterializeResult(
        value=needed_indices,
        metadata={
            "total records": dg.MetadataValue.int(len(needed_indices)),
            }
    )


@dg.asset(
    description="the needed rows from title_basics",
    group_name="intermediates",
    deps=["title_basics", "indices"],
)
def needed_title_basics(
    title_basics=title_basics,
    indices=indices
):

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
            "total records": dg.MetadataValue.int(title_basics_crosstab_genre.shape[0]),
            "not_found": dg.MetadataValue.text(str(not_in_title_basics)),
            }
    )


@dg.asset(
    description="the needed rows from title_ratings",
    group_name="intermediates",
    deps=["title_ratings", "indices"],
)
def needed_title_ratings(
    title_ratings=title_ratings,
    indices=indices
):

    # Get missing indices
    not_in_title_ratings = indices.difference(title_ratings.index).to_list()

    # Get subset with existing indices
    needed_title_ratings = title_ratings.loc[indices.intersection(title_ratings.index)]

    # return not_in_title_ratings, title_ratings # how do i return two variables?


    return dg.MaterializeResult(
        value=needed_title_ratings,
        metadata={
            "total records": dg.MetadataValue.int(needed_title_ratings.shape[0]),
            "not_found": dg.MetadataValue.text(str(not_in_title_ratings)),
            }
    )