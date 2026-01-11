import dagster as dg
import pandas as pd

from src.imdb_dagster.defs.assets import constants
from .transformation import my_movie_list, my_movie_reviews


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