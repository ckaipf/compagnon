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
    import datameta_client

    cogdat_fetcher = CogdatFetcher(datameta_client)
    response = cogdat_fetcher.list()

    return [(record) for record in response]


def get_foreign_ids(
    uow: AbstractUnitOfWork,
) -> List[str]:
    with uow:
        return [record.foreign_id for record in uow.records.list()]


# "YniXRJLVVFRpIHhYwlnLlo1dNNpX2DlF4OoEft3yl3mjg8MPKltaIpi7Lg7e6P7s"
# def create_kraken_execution() -> model.KrakenExecutionFactory:
#     execution = model.KrakenExecutionFactory
#     execution.add_execution_name("Kraken")
#     execution.add_command(lambda x: x["x"] + 1)
#     execution.add_result_parser(lambda x: {"x": x})
#     execution.add_data_parser(lambda x: x)
#     return execution
