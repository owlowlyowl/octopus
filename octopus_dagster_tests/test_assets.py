from dagster import build_asset_context, validate_run_config

from octopus_dagster.assets import step_x, step_y
from octopus_dagster.resources import OctopusResource, SqliteInMemoryDatabase
from octopus_dagster import defs


def test_asset_step_a():
    step_x(
        build_asset_context(),
        octopus=OctopusResource(database=SqliteInMemoryDatabase())
    )


def test_asset_step_b():
    step_y(
        build_asset_context(),
        octopus=OctopusResource(database=SqliteInMemoryDatabase())
    )


def test_job():
    result = defs.get_job_def("model_run").execute_in_process()
    assert result.success


def test_job_with_partition():
    result = (
        defs
        .get_job_def("model_run")
        .execute_in_process(
            # partition_key=["test_partition", "baseline"],
            partition_key="2024-01-02",
        )
    )
    assert result.success
