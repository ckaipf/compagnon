import typer

app = typer.Typer()


@app.command()
def test():
    return


@app.callback()
def main():
    pass


if __name__ == "__main__":
    app()
