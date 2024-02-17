from dagster import build_asset_context

from octopus_dagster.assets import step_a, step_b
from octopus_dagster.resources import OctopusResource, SqliteInMemoryDatabase
from octopus_dagster import model_run_job, defs


def test_asset_step_a():
    step_a(build_asset_context(), octopus=OctopusResource(database=SqliteInMemoryDatabase()))


def test_asset_step_b():
    step_b(build_asset_context(), octopus=OctopusResource(database=SqliteInMemoryDatabase()))


def test_job():
    result = defs.get_job_def("model_run").execute_in_process()
    assert result.success