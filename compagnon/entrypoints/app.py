import typer

import compagnon.service_layer.batchables as batchables
import compagnon.service_layer.services as services
from compagnon.fetchers.fetchers import CogdatFetcher
from compagnon.service_layer.executions.kraken import KrakenExecution
from compagnon.service_layer.unit_of_work import YamlUnitOfWork

app = typer.Typer()

@app.command()
def test():
    uow = YamlUnitOfWork("test.yml")
    batchables.add_missing_records_from_remote(uow, CogdatFetcher())
    services.add_execution_to_records(
        uow,
        KrakenExecution,
    )
    batchables.execute_executions(uow)
    return


@app.callback()
def main():
    """ """


if __name__ == "__main__":
    app.app()
