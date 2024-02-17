from datetime import datetime

import pandas as pd
from dagster import (
    asset,
    MultiPartitionsDefinition, DailyPartitionsDefinition, StaticPartitionsDefinition
)
from sqlalchemy import text

from octopus_dagster.resources import OctopusResource


partition = DailyPartitionsDefinition(start_date=datetime(2024, 1, 1))
# partition = MultiPartitionsDefinition(
#     {
#         "date": DailyPartitionsDefinition(start_date=datetime(2024, 1, 1)),
#         "scenario": StaticPartitionsDefinition(["baseline", "high", "low"]),
#     }
# )


@asset(group_name="octopus", partitions_def=partition)
def step_x(context, octopus: OctopusResource) -> str:
    octopus.persist_scalar_result(context, table_name="step_a", value="ASLKJSDFPSFSM")
    return "step_a_value"


# @asset(group_name="octopus", deps=[step_a])
@asset(group_name="octopus", deps=[step_x], partitions_def=partition)
def step_y(context, octopus: OctopusResource) -> str:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    octopus.persist_dataframe_result(context, table_name="step_b", dataframe=df)
    return "step_b_value"


@asset(group_name="octopus", partitions_def=partition)
def step_with_partition(context, octopus: OctopusResource) -> str:
    value = "oweiruzxcmv"
    octopus.persist_scalar_result(
        context, table_name="step_with_partition", value=value
    )
    return value


@asset(group_name="octopus", deps=[step_y])
def results(context, octopus: OctopusResource) -> None:
    with octopus.database.engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM run"))
        context.log.info(pd.DataFrame(result))
        result = conn.execute(text("SELECT * FROM step_a"))
        context.log.info(pd.DataFrame(result))
        result = conn.execute(text("SELECT * FROM step_b"))
        context.log.info(pd.DataFrame(result))
        result = conn.execute(text("SELECT * FROM step_with_partition"))
        context.log.info(pd.DataFrame(result))
