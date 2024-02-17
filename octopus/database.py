from typing import List, Optional
from uuid import UUID

import pandas as pd
from sqlalchemy import ForeignKey, String, create_engine, Uuid, insert, Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session


class Base(DeclarativeBase):
    pass


def create_all(engine: Engine) -> None:
    Base.metadata.create_all(engine)


class Run(Base):
    __tablename__ = "run"
    id: Mapped[int] = mapped_column(primary_key=True)
    dagster_id: Mapped[UUID] = mapped_column(Uuid())
    name: Mapped[str] = mapped_column(String(30))

    def __repr__(self) -> str:
        return f"Run(id={self.id!r}, name={self.name!r})"


def persist_run(engine, dagster_id: str, name: str) -> int:
    with Session(engine) as session:

        # does a run already exist for this dagster_id?
        run = session.query(Run).filter_by(dagster_id=UUID(dagster_id)).scalar()
        if run is not None:
            # it does exist, so return the run_id
            return run.id

        # it does not exist, so create a new run
        run_id = session.scalar(
            insert(Run).returning(Run.id),
            dict(dagster_id=UUID(dagster_id), name=name),
        )
        session.commit()
        if run_id is None:
            raise ValueError("run_id is None")
        return run_id


def persist_result(dataframe: pd.DataFrame, table_name: str, engine) -> None:
    dataframe.to_sql(table_name, engine, if_exists="append", index=False)