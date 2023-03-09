from sqlalchemy import (
    Column,
    Date,
    ForeignKey,
    Integer,
    MetaData,
    PickleType,
    String,
    Table,
)
from sqlalchemy.orm import registry, relationship

import compagnon.domain.model as model

metadata = MetaData()
mapper_registry = registry()


record_table = Table(
    "records",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("foreign_id", String(255)),
    Column("data", PickleType, nullable=True),
)

execution_table = Table(
    "executions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("result", PickleType, nullable=True),
    Column("record_id", ForeignKey("records.id")),
)


def start_mappers():
    a = mapper_registry.map_imperatively(model.AbstractExecution, execution_table)
    mapper_registry.map_imperatively(
        model.Record,
        record_table,
        properties={"executions": relationship(a)},
    )
