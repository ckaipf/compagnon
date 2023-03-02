import typer

app = typer.Typer()

import compagnon.service_layer.batchables as batchables
from compagnon.fetchers.fetchers import CogdatFetcher
from compagnon.service_layer.unit_of_work import YamlUnitOfWork


@app.command()
def test():
    batchables.add_missing_records_from_remote(
        YamlUnitOfWork("test.yml"), CogdatFetcher()
    )
    return


@app.callback()
def main():
    """
    Format validation.
    """


if __name__ == "__main__":
    app.app()
