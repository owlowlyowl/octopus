from typing_extensions import override

import pandas as pd
from sqlalchemy import create_engine, Engine
from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
from pydantic import PrivateAttr

from octopus.database import create_all, persist_run, persist_result


class SqliteDatabase(ConfigurableResource):

    path: str
    _engine: Engine = PrivateAttr()

    @property
    def engine(self) -> Engine:
        return self._engine

    @override
    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._engine = create_engine(f"sqlite:///{self.path}", echo=True)
        create_all(self._engine)


class SqliteInMemoryDatabase(ConfigurableResource):

    _engine: Engine = PrivateAttr()

    @property
    def engine(self) -> Engine:
        return self._engine

    @override
    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._engine = create_engine("sqlite://", echo=True)
        create_all(self._engine)


class OctopusResource(ConfigurableResource):

    database: ResourceDependency[SqliteInMemoryDatabase]
    _run_id: int = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._run_id = persist_run(
            engine=self.database.engine,
            dagster_id=context.dagster_run.run_id, 
            name=context.dagster_run.job_name,
        )

    def persist_run(self, context) -> int:
        context.log.info(f"persist_run [BEGIN] {context.run.run_id!r}")
        self._run_id = persist_run(
            engine=self.database.engine,
            dagster_id=context.run.run_id, 
            name=context.run.job_name,
        )
        context.log.info(f"persist_run [END] {context.run.run_id!r}")
        return self._run_id

    def persist_scalar_result(self, context, table_name: str, value: str):
        if self._run_id is None:
            self._run_id = self.persist_run(context)

        dataframe = pd.DataFrame({"value": [value]})
        dataframe["run_id"] = self._run_id

        persist_result(dataframe, table_name, self.database.engine)

    def persist_dataframe_result(self, context, table_name: str, dataframe: pd.DataFrame):
        if self._run_id is None:
            self._run_id = self.persist_run(context)

        dataframe["run_id"] = self._run_id
        persist_result(dataframe, table_name, self.database.engine)


