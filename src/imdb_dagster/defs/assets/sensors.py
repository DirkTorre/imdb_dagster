# check if watched_dates_and_scores file changed, if so execute job (create excel, create visualisation)
# check if watch_status file changed, if so execute job (create excel, create visualisation)
# check if title_basics excists, if not => download job
# check if title_ratings excists, if not => download job

import dagster as dg
import os
import time
from datetime import datetime

from src.imdb_dagster.defs.assets import constants
from . import jobs
from .data_assets import raw_inputs


def file_download_sensor(
    asset_to_refresh: dg.AssetsDefinition,
    file_path: str,
    sensor_name: str,
    stale_after_hours: int = 24,
    check_interval_seconds: int = 3600
) -> dg.SensorDefinition:
    """Factory to create file freshness sensors for different assets."""
    
    @dg.sensor(
        name=sensor_name,
        target=[asset_to_refresh],
        minimum_interval_seconds=check_interval_seconds,
        default_status=dg.DefaultSensorStatus.RUNNING
    )
    def _file_download_sensor(context: dg.SensorEvaluationContext):
        """Check if the file needs to be refreshed."""
        
        if not os.path.exists(file_path):
            context.log.info(f"File {file_path} doesn't exist, triggering download")
            return dg.RunRequest(run_key=f"{sensor_name}_missing_{datetime.now().isoformat()}")
        
        mod_time = os.path.getmtime(file_path)
        current_time = time.time()
        hours_old = (current_time - mod_time) / 3600
        
        if hours_old > stale_after_hours:
            context.log.info(f"File {file_path} is {hours_old:.1f} hours old, triggering refresh")
            return dg.RunRequest(run_key=f"{sensor_name}_stale_{datetime.now().isoformat()}")
        else:
            return dg.SkipReason(f"File is only {hours_old:.1f} hours old, still fresh")
    
    return _file_download_sensor


# Create sensors using the factory
title_basics_sensor = file_download_sensor(
    asset_to_refresh=raw_inputs.title_basics,
    file_path=constants.TITLE_BASICS_FILE_PATH,
    sensor_name="title_basics_freshness_sensor",
    stale_after_hours=24,
    check_interval_seconds=60*5
)

title_ratings_sensor = file_download_sensor(
    asset_to_refresh=raw_inputs.title_ratings,
    file_path=constants.TITLE_RATINGS_FILE_PATH,
    sensor_name="title_ratings_freshness_sensor",
    stale_after_hours=24,
    check_interval_seconds=60*5
)


def create_file_change_sensor(
    sensor_name: str,
    job,
    file_path: str,
    minimum_interval_seconds: int = 30
):
    """
    Factory function to create file change sensors.
    
    Args:
        sensor_name: Name of the sensor
        job: The Dagster job to trigger
        file_path: Path to the file to monitor
        minimum_interval_seconds: Minimum seconds between sensor evaluations
    
    Returns:
        A configured sensor definition
    """
    @dg.sensor(
        name=sensor_name,
        job=job,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def file_change_sensor(context):
        # Get the last modification time from cursor (0 if first run)
        last_mtime = float(context.cursor) if context.cursor else 0
        
        if os.path.exists(file_path):
            # Get current file modification time
            current_mtime = os.stat(file_path).st_mtime
            
            # Check if file was modified since last check
            if current_mtime > last_mtime:
                # File has changed, trigger a run
                run_key = f"file_change_{current_mtime}"
                yield dg.RunRequest(run_key=run_key)
                
                # Update cursor with new modification time
                context.update_cursor(str(current_mtime))
    
    return file_change_sensor

# Create your sensors using the factory
file_change_sensor_watch_status = create_file_change_sensor(
    sensor_name="file_change_sensor_watch_status",
    job=jobs.watch_status_job,
    file_path=constants.STATUS_FILE_PATH
)

file_change_sensor_watched_dates_and_scores = create_file_change_sensor(
    sensor_name="file_change_sensor_watched_dates_and_scores",
    job=jobs.watched_dates_and_scores_job,
    file_path=constants.DATES_AND_SCORES_FILE_PATH
)

