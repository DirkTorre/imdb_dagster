this is the new way of loading assets, it's prefered over the old way where every asset must be loaded indiviualy

```python
from pathlib import Path

from dagster import definitions, load_from_defs_folder


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
```


see what's loading form my project

```bash
dg list defs
```


```bash
Using temporary directory /media/user/Data/dirkv/Code/dagster/imdb_dagster/.tmp_dagster_home_2ikiqii6 for storage. This will be removed when dagster dev exits.
2026-01-07 20:53:06 +0100 - dagster - INFO - To persist information across sessions, set the environment variable DAGSTER_HOME to a directory to use.
```

in the dagster gui it's possible to select upstream assets and then materialize unsynced assets.: go to the asset, go to asset lineage, select downstream (all), pres the drop down button next to materialize all and select only materialize unsynced.

>>>> DON'T FORGET TO TURN ALL AUTOMATIONS IN THE DAGSTER GUI


# assets

## automatic materializing

I you use `automation_condition=dg.AutomationCondition.eager()`, materializing starts if a upstream asset changes.

But the stupid thing is that the very first asset doesn't automaticaly materialize when you use `.onmissing()`. Instead you must force the asset to check every x time interval: `automation_condition=dg.AutomationCondition.on_cron("* * * * *") & dg.AutomationCondition.on_missing()`.

I think it doesn't work either, just use the logic i made with file_change_sensor_watch_status().

"Unsynced" detection: Assets are considered unsynced when:
- The code version of the asset has changed
- The dependencies of the asset have changed
- The data version of a parent asset has changed due to a new materialization
