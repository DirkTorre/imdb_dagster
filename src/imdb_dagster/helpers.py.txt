import bokeh.models as models
import bokeh.plotting as plotting
import bokeh.layouts as layout
import pandas as pd


def create_movie_recommendations(final_status):
    """
    Generate Bokeh visualizations for unwatched movies.
    File with visualizations is stored in main.html in the root folder.
    This file stands on its own and can be shared.

    Parameters
    ----------
    final_status : pd.DataFrame
        Contains status information of every movie in the watch list.
    """
    # Transform priority into a string format
    final_status.loc[:,"priority"] = final_status["priority"].astype(pd.BooleanDtype()).map({True: "y", False: "n"})
    final_status["url"] = (
        "https://www.imdb.com/title/" + final_status.index.astype(str) + "/"
    )

    size = (final_status["numVotes"].max() - final_status["numVotes"].min()) / 100
    x_min = final_status["numVotes"].min() - 2*size
    x_max = final_status["numVotes"].max() + 2*size
    y_min = final_status["averageRating"].min()
    y_max = final_status["averageRating"].max()

    # Filter unwatched movies
    source = models.ColumnDataSource(final_status)

    # Create filters and views
    view_priority = models.CDSView(
        filter=models.GroupFilter(column_name="priority", group="y")
    )
    view_all = models.CDSView()

    # Tooltip setup
    tooltips = [("Title", "@primaryTitle"), ("Year", "@startYear")]

    def create_figure(title, view, color):
        """Helper function to generate a scatter plot."""
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

    # Create figures
    fig_all = create_figure("Unwatched Movies", view_all, "blue")
    fig_prio = create_figure("Priority Unwatched Movies", view_priority, "green")

    # Tab layout
    all_tabs = models.Tabs(
        tabs=[
            models.TabPanel(child=fig_prio, title="Priority Movies"),
            models.TabPanel(child=fig_all, title="All Movies"),
        ]
    )

    # Process genre data
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

    # Prepare display data
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

    # Setup data table
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

    # Create headers
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

    # Render visualization
    plotting.show(layout.column(header1, all_tabs, header2, data_cube))
