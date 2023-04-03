import click
from cmkts_server.kraken import KrakenEtlLoader


@click.command()
def hello():
    click.echo("Hello World!!!!")
    loader = KrakenEtlLoader(start="2023-01-01")
    loader.extract()
