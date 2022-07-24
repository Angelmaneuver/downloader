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
    with open(Path.cwd().joinpath('config', 'downloader.yml'), 'r') as file:
        yaml = safe_load(file.read())
        config = yaml['input']['csv']

    input_path = Path(config['path'])

    if not input_path.is_absolute():
        input_path = Path.cwd().joinpath(config['path'])

    output_path = Path(yaml['output']['directory']['path'])

    if not output_path.is_absolute():
        output_path = Path.cwd().joinpath(yaml['output']['directory']['path'])

    if yaml['output']['directory']['clean']:
        rmtree(output_path)
        mkdir(output_path)

    url_column = config['url_column']

    with open(input_path, 'r') as file:
        records = tuple(csv.DictReader(file))

    downloads = []

    for record in records:
        for url in record[url_column].split(','):
            if len(url) > 0:
                filename = Path(urlparse(url).path).name

                downloads.append(Download(
                    url=url,
                    path=output_path.joinpath(filename)
                ))

    return list(set(downloads))


@task(name='Download')
def download(download: Download) -> None:
    response = requests.get(download.url)

    with open(download.path, 'wb') as file:
        file.write(response.content)


with Flow(name='Downloader') as flow:
    download.map(initialize())


def run():
    flow.run()


if '__main__' == __name__:
    run()
