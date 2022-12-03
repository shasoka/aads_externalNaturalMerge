"""
Шенберг Аркадий Алексеевич. КИ21-17/1Б. Практическая работа №5.
    Естественное слияние.

Модуль-CLI для скрипта внешней сортировки слиянием.
"""

import os
from typing import Optional

import click

from external_sort.reader import StrTup
from external_sort.sort import sort_hub


@click.command()
@click.argument("src", nargs=-1, type=click.Path(exists=True, file_okay=True))
@click.option("--type_data", '-td', type=click.Choice(['i', 'f', 's']),
              multiple=True, default=None,
              help="Expected type of files' content. If it wasn't passed,"
                   " type(-s) will be casted automatically via literal "
                   "evaluation.")
@click.option("--output", "-o", type=click.Path(exists=False),
              default=None, help="Output file.")
@click.option("--reverse", "-r", is_flag=True, default=False,
              type=bool, help="Flag for reverse sorting.")
@click.option("--nflows", "-nf", type=click.IntRange(min=1), default=None,
              help="Number of threads for multithreaded run.")
@click.option("--keys", "-k", type=str, multiple=True, default=None,
              help="Keys for .csv file. If it wasn't passed, sorting will"
                   " occur by all keys.")
@click.option("--delimiter", "-d", type=str, default=',', help="Delimiter for "
                                                               ".csv table.")
def cli(src: list[str], type_data: StrTup, output: Optional[str],
        reverse: Optional[bool], nflows: Optional[int],
        keys: Optional[tuple[str, ...]], delimiter: str) -> None:
    """
    \b
    Welcome to external natural merge sorter!
    It takes one positional argument:
    -----\b
    SRC: A list of source files which you want to sort.
    """

    # Получаемый тип - tuple
    src = list(src)

    # Проверка на существование директории для output
    if output and (folder := output.removesuffix(os.path.basename(output))):
        if not os.path.isdir(folder):
            raise FileNotFoundError("Given directory does not exists")

    if not type_data:
        click.secho("!Type data parameter was not passed. It will be "
                    "calculated via literal evaluating!", fg="yellow")

    if os.path.splitext(os.path.basename(src[0]))[1] == ".csv" and not keys:
        click.secho("!Keys parameter wasn't passed. Sorting will occur by all "
                    "keys!", fg="yellow")

    sort_hub(src, type_data, output, reverse, nflows, None, keys, delimiter)
    click.secho("-/ DONE /-", fg="green")


if __name__ == '__main__':
    cli()
