# imdb_dagster

A simple version of my imdb project to generate a excel sheet of movie information for my movie list.

## Getting started

### Installing dependencies

**Option 1: uv**

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
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)

## Todo

1. clean the code (remove dead code (are the jobs and schedules still needed?))
2. document code (add type annotation)
3. finish course

## Done

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