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

look at the on eager thing: https://docs.dagster.io/guides/automate/declarative-automation

constante unsynced_condition werkt niet meer (documentatie lezen), (en mischien is de job voor excel niet meer nodig)


"Unsynced" detection: Assets are considered unsynced when 2 :

The code version of the asset has changed
The dependencies of the asset have changed
The data version of a parent asset has changed due to a new materialization




- the output files don't say unsynced when some things upstream are unsynced.......

3. create job for generating the excel file (elke 5 minuten elke niet gesyncde upstream assets ) << later
4. document code
5. finish course

## Done
 
elke 5 minute kijkt de sensor of de file op schijf meer dan 24 uur oud is, zo ja dan wordt de asset voor de ruwe imdb files gerund.
de assets voor het downloaden checkt of de files al op de schijf staan en of deze ouder zijn dan 24 uur, zo ja dan worden de files gedownload.

- clean up code (remove redundancy) (verder met helpers. de rest heb ik al gedaan)
- automatic download of files every day + create freshness check : https://docs.dagster.io/guides/test/data-freshness-testing + https://docs.dagster.io/guides/observe/asset-freshness-policies
- fix the problems with the checks
- add the visualisation
- add better dataframe descriptions in assets