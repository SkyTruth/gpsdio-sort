"""
Unittests for `gpsdio_sort.core`.
"""


import tempfile

from click.testing import CliRunner
import gpsdio_sort.core
import gpsdio.cli
import os.path
import datetime
import random

def randdate():
    return datetime.datetime(
        2014, 1+int(12*random.random()),
        1+int(28*random.random()),
        int(24*random.random()),
        int(60*random.random()),
        int(60*random.random())
        )

def cleanup():
    if os.path.exists('in.msg'):
        os.unlink('in.msg')
    if os.path.exists('out.msg'):
        os.unlink('out.msg')


def test_sort():
    cleanup()
    try:
        with gpsdio.open("in.msg", "w") as f:
            for i in range(0, 1000):
                f.writerow({'timestamp': randdate(), 'lat': 180*random.random()-90.0, 'lon': 360*random.random()-180.0})

        result = CliRunner().invoke(gpsdio.cli.main.main_group, [
            '--i-drv', 'MsgPack',
            '--o-drv', 'MsgPack',
            'sort',
            '-c', 'timestamp',
            'in.msg',
            'out.msg',
        ])
        
        last = None
        with gpsdio.open('out.msg') as f:
            for row in f:
                if last is not None:
                    print(row)
                    assert last['timestamp'] < row['timestamp']
                last = row
    finally:
        cleanup()
