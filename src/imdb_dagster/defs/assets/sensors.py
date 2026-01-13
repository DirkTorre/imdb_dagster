# check if watched_dates_and_scores file changed, if so execute job (create excel, create visualisation)
# check if watch_status file changed, if so execute job (create excel, create visualisation)
# check if title_basics excists, if not => download job
# check if title_ratings excists, if not => download job

import dagster as dg
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.imdb_dagster.defs.assets import constants
from . import jobs
from .data_assets import raw_inputs


def file_download_sensor(
    asset_to_refresh: dg.AssetsDefinition,
    file_path: str,
    sensor_name: str,
    stale_after_hours: int = 24,
    check_interval_seconds: int = 3600,
) -> dg.SensorDefinition:
    """Factory to create file freshness sensors for different assets.

    Triggers a run when the file is missing or older than stale_after_hours.
    """
    path = Path(file_path)

    @dg.sensor(
        name=sensor_name,
        target=[asset_to_refresh],
        minimum_interval_seconds=check_interval_seconds,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def _file_download_sensor(context: dg.SensorEvaluationContext):
        """Check if the file needs to be refreshed."""
        try:
            if not path.exists():
                context.log.info(f"File {path} does not exist -> triggering download")
                return dg.RunRequest(
                    run_key=f"{sensor_name}_missing_{datetime.now().isoformat()}"
                )

            mod_time = path.stat().st_mtime
            current_time = time.time()
            hours_old = (current_time - mod_time) / 3600

            if hours_old > stale_after_hours:
                context.log.info(
                    f"File {path} is {hours_old:.1f} hours old (> {stale_after_hours}h) -> triggering refresh"
                )
                return dg.RunRequest(
                    run_key=f"{sensor_name}_stale_{datetime.now().isoformat()}"
                )

            context.log.debug(f"File {path} is fresh ({hours_old:.1f} hours old)")
            return dg.SkipReason(f"File is only {hours_old:.1f} hours old, still fresh")
        except Exception as exc:
            context.log.error(f"Error while evaluating sensor {sensor_name}: {exc}")
            # In case of unexpected error, skip and surface message
            return dg.SkipReason(f"Sensor error: {exc}")

    return _file_download_sensor


# Create sensors using the factory
title_basics_sensor = file_download_sensor(
    asset_to_refresh=raw_inputs.title_basics,
    file_path=constants.TITLE_BASICS_FILE_PATH,
    sensor_name="title_basics_freshness_sensor",
    stale_after_hours=24,
    check_interval_seconds=60 * 5,
)

title_ratings_sensor = file_download_sensor(
    asset_to_refresh=raw_inputs.title_ratings,
    file_path=constants.TITLE_RATINGS_FILE_PATH,
    sensor_name="title_ratings_freshness_sensor",
    stale_after_hours=24,
    check_interval_seconds=60 * 5,
)


def create_file_change_sensor(
    sensor_name: str,
    job: dg.JobDefinition,
    file_path: str,
    minimum_interval_seconds: int = 30,
) -> dg.SensorDefinition:
    """
    Factory function to create file change sensors.

    Args:
        sensor_name: Name of the sensor
        job: The Dagster job to trigger
        file_path: Path to the file to monitor
        minimum_interval_seconds: Minimum seconds between sensor evaluations

    Returns:
        A configured sensor definition that returns either a RunRequest or SkipReason.
    """
    path = Path(file_path)

    @dg.sensor(
        name=sensor_name,
        job=job,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def file_change_sensor(context: dg.SensorEvaluationContext):
        try:
            last_mtime = float(context.cursor) if context.cursor else 0.0

            if path.exists():
                current_mtime = path.stat().st_mtime

                if current_mtime > last_mtime:
                    run_key = f"{sensor_name}_file_change_{int(current_mtime)}"
                    context.log.info(
                        f"Detected change in {path} -> triggering job {job.name}"
                    )
                    context.update_cursor(str(current_mtime))
                    return dg.RunRequest(run_key=run_key)

                context.log.debug(
                    f"No change detected for {path} (mtime {current_mtime})"
                )
                return dg.SkipReason("No change detected")

            # File does not exist
            if last_mtime != 0.0:
                # File existed previously but is now missing -> trigger job to handle removal
                context.log.info(
                    f"File {path} was removed since last check -> triggering job {job.name}"
                )
                context.update_cursor("0")
                return dg.RunRequest(
                    run_key=f"{sensor_name}_missing_{datetime.now().isoformat()}"
                )

            context.log.debug(
                f"File {path} missing and never seen before; nothing to do"
            )
            return dg.SkipReason("File missing and no previous record")

        except Exception as exc:
            context.log.error(
                f"Error evaluating file change sensor {sensor_name}: {exc}"
            )
            return dg.SkipReason(f"Sensor error: {exc}")

    return file_change_sensor


# Create your sensors using the factory
file_change_sensor_watch_status = create_file_change_sensor(
    sensor_name="file_change_sensor_watch_status",
    job=jobs.watch_status_job,
    file_path=constants.STATUS_FILE_PATH,
)

file_change_sensor_watched_dates_and_scores = create_file_change_sensor(
    sensor_name="file_change_sensor_watched_dates_and_scores",
    job=jobs.watched_dates_and_scores_job,
    file_path=constants.DATES_AND_SCORES_FILE_PATH,
)
