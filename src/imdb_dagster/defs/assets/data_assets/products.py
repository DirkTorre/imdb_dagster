import dagster as dg
import pandas as pd

from src.imdb_dagster.defs.assets import constants
from .transformation import my_movie_list, my_movie_reviews
from .... import helpers


@dg.asset(
    description="The excel sheet that can be easily filtered and shared",
    group_name="products",
    deps=["my_movie_list", "my_movie_reviews"],
)
def watch_list_excel(my_movie_list, my_movie_reviews):
    with pd.ExcelWriter(
        constants.PRODUCT_EXCEL_FILE_PATH, engine="xlsxwriter"
    ) as writer:
        my_movie_list.to_excel(writer, sheet_name="Movie List")
        my_movie_reviews.to_excel(writer, sheet_name="Dates and Reviews")

    return dg.MaterializeResult(
        # value="",
        metadata={"file path": dg.MetadataValue.path(constants.PRODUCT_EXCEL_FILE_PATH)}
    )




@dg.asset(
    description="HTML visualisations of unwatched movies.",
    group_name="products",
    deps=["my_movie_list"],
)
def watch_list_figure_html(my_movie_list):
    html_path = constants.PRODUCT_FIGURE_FILE_PATH
    helpers.create_movie_recommendations(my_movie_list, html_path)

    return dg.MaterializeResult(
        metadata={"file path": dg.MetadataValue.path(html_path)}
    )