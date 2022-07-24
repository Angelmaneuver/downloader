#!/usr/bin/env python3
from typing import NamedTuple
from pathlib import Path


class Download(NamedTuple):
    url:  str
    path: Path
