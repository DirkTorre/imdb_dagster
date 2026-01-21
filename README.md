# imdb_dagster

Lightweight Dagster project for building and materializing an IMDb-based watchlist pipeline. The codebase is asset-centric and includes raw inputs, processing, intermediate enrichment, and outputs (Excel + HTML visualizations). Sensors and schedules are used to drive updates; assets can be configured to only materialize when out-of-sync.

Contents
- Overview
- Key concepts
- Quickstart (Linux)
- Running & scheduling
- Project layout
- Learn more
- Task list


## Overview

This repository implements an asset-based Dagster pipeline that:
- Ingests IMDb raw files and handmade CSVs
- Processes/merges data into intermediate assets
- Generates output artifacts: an Excel watchlist and an HTML visualization
- Uses sensors and automation conditions so updates only run for assets that are out-of-sync

## Key concepts

- Assets: defined under `src/imdb_dagster/defs/assets` as Dagster @asset definitions.
- Jobs: asset jobs that select output assets and upstream dependencies are defined in `jobs.py`.
- Sensors: file- and upstream-change sensors live in `sensors.py` and can trigger jobs when needed.
- Automation conditions: use an `unsynced_condition` (see `constants.py`) so a job run will materialize only assets that are stale or missing.
- Helpers: utility routines live in `src/imdb_dagster/helpers.py` (file I/O, download, and viz helpers).

## Quickstart (Linux)

### Installing dependencies

**Option 1: uv (recommended)**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -r requirements.txt
```

### Running & scheduling

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

Head over to 'Automation' and turn on all sensors.

The pipeline should execute automaticaly.
The files 'date_scores.csv' and 'status.csv' are checked every 5 minutes for (saved) changes. If one of the files has changed it wil refresh assets to create up-to-date output files.

There are two common patterns to update assets periodically:

1) Scheduled asset job (recommended for periodic runs)
- Define an asset job selecting your output assets (and upstreams) and attach a ScheduleDefinition with a cron expression.

Example: run the output job every 15 minutes (place in `defs/assets/schedules.py` or similar)
```python
# filepath: src/imdb_dagster/defs/assets/schedules.py
import dagster as dg
from .jobs import watch_list_job  # a job that selects the output assets

watch_list_schedule = dg.ScheduleDefinition(
    name="watch_list_schedule",
    job=watch_list_job,
    cron_schedule="*/15 * * * *",  # every 15 minutes
)
```

2) Sensors + automation conditions
- Use sensors to detect file changes (or upstream materializations) and trigger jobs.
- Ensure assets define an automation_condition that evaluates to True only when the asset is missing or out-of-sync (see `constants.unsynced_condition` pattern in the code).
- Run `dagster-daemon` so sensors are evaluated continuously.

Automation (only run unsynced assets)
- The combination of running an asset job + setting an asset's `automation_condition` to an "unsynced" condition will cause Dagster to skip materializing assets that are up-to-date and only run the ones that need refresh.
- Check `src/imdb_dagster/defs/assets/constants.py` for an example `unsynced_condition` combining checks like `missing()`, `any_deps_updated()` and `cron_tick_passed()`.


## Project layout (important files)

- src/imdb_dagster/defs/assets/
  - constants.py         — file paths and automation condition helpers
  - jobs.py              — define_asset_job selections for asset jobs
  - sensors.py           — file-change and upstream-change sensors
  - schedules.py         — schedule definitions (optional)
  - data_assets/
    - raw_inputs.py
    - inputs.py
    - intermediates.py
    - outputs.py         — Excel and HTML output assets
    - checks.py
- src/imdb_dagster/helpers.py — utilities for IO, downloads, and visualizations
- requirements.txt


## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)

## Task list

### Todo

1. finish course

### Done

- clean the code (remove dead code (are the jobs and schedules still needed?))
- document code (add type annotation)
- make a warning the handmade input files have the wrong amount of comma's / columns
- title_basics and title_ratings are loaded automatically 
- all files are updated automatically when an input changes.
- all inputs are loaded automatically
Every 5 minutes, the sensor checks whether the file on disk is more than 24 hours old. If so, the asset for the raw IMDb files is run.
The assets for downloading check whether the files are already on the disk and whether they are older than 24 hours. If so, the files are downloaded.

- Clean up code (remove redundancy) (continue with helpers. I have already done the rest)
- Automatic download of files every day + create freshness check: https://docs.dagster.io/guides/test/data-freshness-testing + https://docs.dagster.io/guides/observe/asset-freshness-policies
- Fix the problems with the checks
- Add the visualization
- Add better dataframe descriptions in assets



