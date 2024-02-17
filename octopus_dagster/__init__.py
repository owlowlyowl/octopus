from dagster import Definitions, load_assets_from_modules, AssetSelection, define_asset_job

from octopus_dagster import assets
from octopus_dagster.resources import OctopusResource, SqliteInMemoryDatabase, SqliteDatabase


all_assets = load_assets_from_modules([assets])

model_run_job = define_asset_job(
    "model_run",
    selection=AssetSelection.groups("octopus"),
    partitions_def=assets.partition,
)

defs = Definitions(
    assets=all_assets,
    resources={
        "octopus": OctopusResource(
            # database=SqliteInMemoryDatabase(),
            database=SqliteDatabase(path="c:/workspace/octopus/octopus.db"),
        ),
    },
    jobs=[model_run_job],
)
