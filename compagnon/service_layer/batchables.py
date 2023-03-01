import functools

import compagnon.domain.model as model
from compagnon.fetchers.fetchers import AbstractFetcher
from compagnon.service_layer.services import (
    add_records,
    compare_local_and_remote,
    fetch_records,
    get_foreign_ids,
)
from compagnon.service_layer.unit_of_work import AbstractUnitOfWork

# The maximal number of iterations a batchable service can run.
# This is to prevent infinite loops by a malformed function.
MAX_ITERATIONS = 1000


def batched(batch_size: int = 10):
    """
    Decorator to batch a service function.
    A batachable service must have a batch_size argument and
    returns a boolean, which is True when the service is finished.
    """

    def decorator_batched(func):
        @functools.wraps(func)
        def wrapper_batched(*args, **kwargs):
            for n in range(MAX_ITERATIONS):
                finished = func(*args, **kwargs, batch_size=batch_size)
                if finished:
                    return n
            return n

        return wrapper_batched

    return decorator_batched


@batched(batch_size=10)
def add_missing_records(
    uow: AbstractUnitOfWork,
    fetcher: AbstractFetcher,
    batch_size: int = 10,
) -> bool:
    finished = False
    if compare_local_and_remote(uow, fetcher):
        with uow:
            remote_records = fetch_records(fetcher)
            local_records = uow.records.list()
            not_added_yet = [
                record
                for record in remote_records
                if record.foreign_id not in get_foreign_ids(local_records)
            ]
            add_records(uow, not_added_yet[:batch_size])
            uow.commit()
    else:
        finished = True
    return finished


@batched(batch_size=10)
def execute_executions(
    uow: AbstractUnitOfWork,
    batch_size: int = 10,
) -> bool:
    with uow:
        records = uow.records.list()
        not_executed_executions = [
            execution
            for record in records
            for execution in record.executions
            if not execution.result
        ]

        if not_executed_executions:
            for execution in not_executed_executions[:batch_size]:
                execution.execute()
            uow.commit()
            return False  # Was not finished
        else:
            return True  # Was finished
