import click
from cmkts_server.kraken import KrakenEtlLoader


@click.command()
def hello():
    click.echo("Hello World!!!!")
    loader = KrakenEtlLoader(start="2023-04-02")
    df = loader.extract()
    print(df)
