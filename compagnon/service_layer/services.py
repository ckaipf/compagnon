import datetime
from typing import Dict, List, Type

import compagnon.domain.model as model
from compagnon.fetchers.fetchers import CogdatFetcher
from compagnon.service_layer.unit_of_work import AbstractUnitOfWork


def add_records(
    records: List[model.Record],
    uow: AbstractUnitOfWork,
) -> str:
    with uow:
        for record in records:
            uow.records.add(record)
        uow.commit()


def add_execution_to_records(
    execution: model.ExecutionFactory,
    uow: AbstractUnitOfWork,
) -> None:
    with uow:
        records = uow.records.list()
        for record in records:
            e = execution(record, datetime.datetime.now())
            record.add_execution(e)
            uow.records.add(record)
        uow.commit()


def get_records_from_datameta() -> List[model.Record]:
    cogdat_fetcher = CogdatFetcher()
    response = cogdat_fetcher.list()

    return [(record) for record in response]


def get_foreign_ids(
    uow: AbstractUnitOfWork,
) -> List[str]:
    with uow:
        return [record.foreign_id for record in uow.records.list()]
