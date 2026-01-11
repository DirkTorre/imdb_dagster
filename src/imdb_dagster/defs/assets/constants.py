import dagster as dg

TITLE_BASICS_FILE_PATH = "data/inputs/imdb_files/title.basics.tsv.gz"
TITLE_RATINGS_FILE_PATH = "data/inputs/imdb_files/title.ratings.tsv.gz"
DATES_AND_SCORES_FILE_PATH = "data/inputs/handmade_files/date_scores.csv"
STATUS_FILE_PATH = "data/inputs/handmade_files/status.csv"
PRODUCT_EXCEL_FILE_PATH = "data/outputs/watch_list.xlsx"
PRODUCT_FIGURE_FILE_PATH = "data/outputs/watch_list.html"
# unsynced_condition = (
#     (
#         dg.AutomationCondition.any_deps_updated()  # Any upstream has updated
#     | dg.AutomationCondition.code_version_changed()  # Code version changed
#     | dg.AutomationCondition.missing()  # Asset never materialized
#     # | dg.AutomationCondition.eager()  # Update when upstreams change
#     ) & ~dg.AutomationCondition.in_progress()  # Not if already running
#     & dg.AutomationCondition.cron_tick_passed(cron_schedule="*/2 * * * *") # only every 2 minutes
# )
# automation_condition=constants.unsynced_condition # bij assets in decorator
