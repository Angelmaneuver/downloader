#!/usr/bin/env python3
import csv
from pathlib import Path
from urllib.parse import urlparse
from os import mkdir
from shutil import rmtree

from prefect import task, Flow
from yaml import safe_load
import requests

from .classes.download import Download


@task(name='Initialize')
def initialize() -> list[Download]:
    input_path, output_path, cleaning, url_column = get_config()

    if cleaning:
        clean(output_path)

    with open(input_path, 'r') as file:
        records = tuple(csv.DictReader(file))

    downloads = []

    for record in records:
        downloads.extend(get_download_target(
            url_column,
            output_path,
            record
        ))

    return list(set(downloads))


@task(name='Download')
def download(download: Download) -> None:
    response = requests.get(download.url)

    with open(download.path, 'wb') as file:
        file.write(response.content)


with Flow(name='Downloader') as flow:
    download.map(initialize())


def get_config() -> tuple[Path, Path, bool, str]:
    with open(Path.cwd().joinpath('config', 'downloader.yml'), 'r') as file:
        yaml = safe_load(file.read())

    input_path = Path(yaml['input']['csv']['path'])

    if not input_path.is_absolute():
        input_path = Path.cwd().joinpath(yaml['input']['csv']['path'])

    output_path = Path(yaml['output']['directory']['path'])

    if not output_path.is_absolute():
        output_path = Path.cwd().joinpath(yaml['output']['directory']['path'])

    return (
        input_path,
        output_path,
        yaml['output']['directory']['clean'],
        yaml['input']['csv']['url_column']
    )


def clean(path: Path) -> None:
    rmtree(path)
    mkdir(path)


def get_download_target(
    url_column: str,
    path: Path,
    record: dict
) -> list[Download]:

    downloads = []

    for url in record[url_column].split(','):
        if len(url) > 0:
            filename = Path(urlparse(url).path).name

            downloads.append(Download(
                url=url,
                path=path.joinpath(filename)
            ))

    return downloads


def run():
    flow.run()


if '__main__' == __name__:
    run()
