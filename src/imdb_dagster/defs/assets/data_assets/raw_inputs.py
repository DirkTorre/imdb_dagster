import dagster as dg
import os
import time
import requests
from datetime import datetime

from src.imdb_dagster.defs.assets import constants


def create_download_asset(
    name: str,
    file_path: str,
    download_url: str,
    description: str,
    stale_after_hours: int = 24,
) -> dg.MaterializeResult:
    """Factory to create file download assets with staleness checking."""

    @dg.asset(
        name=name,
        group_name="raw_inputs",
        description=description,
        automation_condition=dg.AutomationCondition.on_cron("* * * * *")
        & dg.AutomationCondition.on_missing(),  # makes sure it checks every minute if asset exists.
    )
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Download the file only if it's stale or missing."""

        # Check if file exists and is fresh
        if os.path.exists(file_path):
            mod_time: float = os.path.getmtime(file_path)
            current_time: float = time.time()
            hours_old: float = (current_time - mod_time) / 3600

            if hours_old < stale_after_hours:
                context.log.info(
                    f"File {name} is only {hours_old:.1f} hours old, skipping download"
                )
                return dg.MaterializeResult(
                    metadata={
                        "skipped_download": dg.MetadataValue.bool(True),
                        "file_age_hours": dg.MetadataValue.float(hours_old),
                    }
                )

        # File is stale or missing, download it
        context.log.info(f"Downloading fresh {name}")
        response = requests.get(download_url)
        with open(file_path, "wb") as output_file:
            output_file.write(response.content)

        return dg.MaterializeResult(
            metadata={
                "file_size": dg.MetadataValue.int(len(response.content)),
                "download_time": dg.MetadataValue.text(datetime.now().isoformat()),
                "skipped_download": dg.MetadataValue.bool(False),
            }
        )

    return _asset


# Create your assets using the factory
title_basics = create_download_asset(
    name="title_basics_raw",
    file_path=constants.TITLE_BASICS_FILE_PATH,
    download_url="https://datasets.imdbws.com/title.basics.tsv.gz",
    description="Raw IMDB title_basics file",
    stale_after_hours=23,
)

title_ratings = create_download_asset(
    name="title_ratings_raw",
    file_path=constants.TITLE_RATINGS_FILE_PATH,
    download_url="https://datasets.imdbws.com/title.ratings.tsv.gz",
    description="Raw IMDB title_ratings file",
    stale_after_hours=23,
)
