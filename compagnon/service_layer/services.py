import datetime
from typing import Callable, Dict, List, Type

import compagnon.domain.model as model
from compagnon.fetchers.fetchers import AbstractFetcher
from compagnon.service_layer.unit_of_work import AbstractUnitOfWork


class InvalidRecord(Exception):
    pass


class InvalidLocalState(Exception):
    pass


def is_valid_record(record: model.Record, records: List[model.Record]) -> bool:
    if record in records:
        raise InvalidRecord(f"Record {record.foreign_id} already exists")
    return True


def is_valid_execution(execution: model.AbstractExecution) -> bool:
    return True


def add_records(
    records: List[model.Record],
    uow: AbstractUnitOfWork,
):
    with uow:
        for record in records:
            if is_valid_record(record, uow.records.list()):
                uow.records.add(record)
        uow.commit()


def add_execution_to_records(
    execution: model.AbstractExecution,
    uow: AbstractUnitOfWork,
) -> None:
    with uow:
        records = uow.records.list()
        for record in records:
            e = execution(record, datetime.datetime.now())
            if is_valid_execution(e):
                record.add_execution(e)
        uow.commit()


def fetch_records(fetcher: AbstractFetcher) -> List[model.Record]:
    return fetcher.list()


def exist_unseen_records_in_remote(
    uow: AbstractUnitOfWork,
    fetcher: AbstractFetcher,
) -> bool:
    with uow:
        local = set(get_foreign_ids(uow.records.list()))
    retrived = set(get_foreign_ids(fetch_records(fetcher)))

    local_, intersection, retrived_ = (
        bool(local - retrived),
        bool(local & retrived),
        bool(retrived - local),
    )

    if local_:
        raise InvalidLocalState(f"Local records {local_} not in remote")
    if not intersection:
        pass  # TODO: log warning that intersection is empty
    if retrived_:
        return True
    return False


def get_foreign_ids(records: List[model.Record]) -> List[str]:
    return [record.foreign_id for record in records]
