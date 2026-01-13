import bokeh.models as models
import bokeh.plotting as plotting
import bokeh.layouts as layout
from bokeh.io import output_file, save
import pandas as pd
import dagster as dg
from dagster import MetadataValue, TableRecord


def create_movie_recommendations(final_status, filepath):
    """
    Generate Bokeh visualizations for unwatched movies and save to HTML.

    Parameters
    ----------
    final_status : pd.DataFrame
        Contains status information of every movie in the watch list.
    filepath : str
        Full path where the HTML file should be saved.
    """

    # Transform priority into a string format
    final_status.loc[:, "priority"] = (
        final_status["priority"].astype(pd.BooleanDtype()).map({True: "y", False: "n"})
    )
    final_status["url"] = (
        "https://www.imdb.com/title/" + final_status.index.astype(str) + "/"
    )

    size = (final_status["numVotes"].max() - final_status["numVotes"].min()) / 100
    x_min = final_status["numVotes"].min() - 2 * size
    x_max = final_status["numVotes"].max() + 2 * size
    y_min = final_status["averageRating"].min()
    y_max = final_status["averageRating"].max()

    source = models.ColumnDataSource(final_status)

    view_priority = models.CDSView(
        filter=models.GroupFilter(column_name="priority", group="y")
    )
    view_all = models.CDSView()

    tooltips = [("Title", "@primaryTitle"), ("Year", "@startYear")]

    def create_figure(title, view, color):
        fig = plotting.figure(
            title=title,
            x_axis_label="Number of Votes",
            y_axis_label="Average Rating",
            width=1500,
            height=1000,
            y_range=(y_min, y_max),
            x_range=(x_min, x_max),
            tooltips=tooltips,
            tools="tap,box_zoom,pan,wheel_zoom,reset,save",
        )
        fig.circle(
            x="numVotes",
            y="averageRating",
            radius=size,
            alpha=0.5,
            source=source,
            view=view,
            color=color,
        )

        hover = fig.select_one(models.HoverTool)
        hover.point_policy = "follow_mouse"
        hover.tooltips = tooltips

        fig.select_one(models.TapTool).callback = models.OpenURL(url="@url")

        return fig

    fig_all = create_figure("Unwatched Movies", view_all, "blue")
    fig_prio = create_figure("Priority Unwatched Movies", view_priority, "green")

    all_tabs = models.Tabs(
        tabs=[
            models.TabPanel(child=fig_prio, title="Priority Movies"),
            models.TabPanel(child=fig_all, title="All Movies"),
        ]
    )

    genre_columns = [col for col in final_status.columns if "genre" in col]
    genres = (
        final_status[genre_columns]
        .replace(False, pd.NA)
        .stack()
        .reset_index()
        .drop(columns=[0])
        .rename(columns={"level_1": "genre"})
    )
    genres["genre"] = genres["genre"].str.replace("genre_", "")
    genres = genres.set_index("tconst")

    display_status = final_status[~final_status["watched"]].drop(columns=genre_columns)
    display_status = (
        display_status.join(genres)
        .groupby("genre")
        .head(10)
        .sort_values(["genre", "averageRating"], ascending=[True, False])
    )
    display_status = display_status[
        ["averageRating", "primaryTitle", "startYear", "genre"]
    ].round({"averageRating": 1})

    top10 = models.ColumnDataSource(display_status)
    columns = [
        models.TableColumn(field=field, title=title)
        for field, title in [
            ("genre", "Genre"),
            ("primaryTitle", "Title"),
            ("startYear", "Year"),
            ("averageRating", "Rating"),
            ("tconst", "tconst"),
        ]
    ]

    data_cube = models.DataCube(
        source=top10,
        columns=columns,
        grouping=[models.GroupingInfo(getter="genre")],
        target=models.ColumnDataSource(data=dict(row_indices=[], labels=[])),
        width=1500,
        height=1500,
    )

    header1 = models.Div(
        text="""<h1 style="text-align: center">Visualization of Unwatched Movies</h1>
                <ul>
                    <li>Use tabs to switch info.</li>
                    <li>Hover over data points to view movie info.</li>
                    <li>Click a data point to go to IMDb.</li>
                </ul>"""
    )

    header2 = models.Div(
        text="""<h1 style="text-align: center">Top 10 Unwatched Movies per Genre</h1>
                Press the + icon at the genre to unfold the movies."""
    )

    # Build layout
    full_layout = layout.column(header1, all_tabs, header2, data_cube)

    # Save to user-chosen HTML file
    output_file(filepath)
    save(full_layout)


ALL_VALUES = {
    "tconst": "alphanumeric unique identifier of the title",
    "averageRating": "weighted average of all the individual user ratings",
    "numVotes": "number of votes the title has received",
    "titleType": "type/format of the title (movie, short, tvseries, etc)",
    "primaryTitle": "popular title used on promotional materials",
    "originalTitle": "original title in the original language",
    "isAdult": "0 = non‑adult title, 1 = adult title",
    "startYear": "release year; for TV series, the start year",
    "endYear": "TV series end year; '\\N' for other title types",
    "runtimeMinutes": "primary runtime of the title in minutes",
    "genres": "up to three genres associated with the title",
    "watched": "whether the movie has been watched",
    "priority": "whether the movie has priority",
    "netflix": "whether the movie is on Netflix (handmade value, no value means unknown)",
    "prime": "whether the movie is on Amazon Prime (handmade value, no value means unknown)",
    "date": "date the movie was watched",
    "enjoyment_score": "enjoyment score given after watching. 0=no enjoyment; 1=mweh; 2=fun; 3=good/cool; 4=great",
    "quality_score": "quality score given after watching. 0=bad, don't watch; 1=bad but interesting; 2=good engough; 3=good;4=great",
}


def get_table_schema(df: pd.DataFrame, max_preview: int = 10) -> MetadataValue:
    """
    Convert a DataFrame into a Dagster MetadataValue.table with schema and preview records.

    Args:
        df: DataFrame to convert. Index will be reset so preview shows columns as fields.
        max_preview: number of preview rows to include.

    Returns:
        Dagster MetadataValue.table instance describing the schema and sample records.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas.DataFrame")

    preview_df = df.reset_index().head(max_preview).copy()

    columns = []
    for col in preview_df.columns:
        dtype_str = str(preview_df.dtypes[col])
        description = ALL_VALUES.get(col, "unknown")
        # dagster.TableColumn takes (name, type, description) historically
        try:
            columns.append(dg.TableColumn(col, dtype_str, description))
        except TypeError:
            # Fallback: construct via kwargs for different dagster versions
            columns.append(dg.TableColumn(name=col, type=dtype_str, description=description))

    records = []
    # Only include JSON serializable primitives; fallback to string
    for row in preview_df.to_dict(orient="records"):
        cleaned = {
            key: (value if isinstance(value, (str, int, float, bool, type(None))) else str(value))
            for key, value in row.items()
        }
        try:
            records.append(dg.TableRecord(cleaned))
        except Exception:
            # Fallback: use plain dict if TableRecord is unavailable/strict
            records.append(cleaned)

    try:
        schema = dg.TableSchema(columns=columns)
        return MetadataValue.table(records=records, schema=schema)
    except Exception:
        # In case dagster API differs, return minimal table metadata
        return MetadataValue.table(records=records)
