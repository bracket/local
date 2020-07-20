import invoke

from pathlib import Path

FILE = Path(__file__)
HERE = FILE.parent
DATA = HERE / 'data'


@invoke.task
def print_stuff(context):
    iwthh o
