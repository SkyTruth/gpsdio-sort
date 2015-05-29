"""
Core components for gpsdio_sort
"""

import click
import gpsdio
import gpsdio.schema
import msgpack
import subprocess
import os
import datetime

@click.command(name='sort')
@click.option(
    '-c', '--cols', metavar='COLS', default="timestamp",
    help="Sort rows by columns (coma separated list). Default: timestamp",
)
@click.argument("infile", metavar="INPUT_FILENAME")
@click.argument("outfile", metavar="OUTPUT_FILENAME")
@click.pass_context
def gpsdio_sort(ctx, infile, outfile, cols='timestamp'):
    """
    Sorts messages in an arbitrarily large file according to an
    arbitrary set of columns, by default 'timestamp'.
    """

    tempfile1 = outfile + ".tmp1"
    tempfile2 = outfile + ".tmp2"

    cols = cols.split(",")

    def mangle(item):
        if isinstance(item, int):
            return "%020d" % item
        elif isinstance(item, float):
            return "%040.20f" % item
        elif isinstance(item, datetime.datetime):
            return item.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            return unicode(item).encode("utf-8")

    def getKey(row):
        return ' : '.join(mangle(row.get(col, '')) for col in cols)

    def format_row(row):
        return msgpack.dumps(
            gpsdio.schema.export_msg(row)
            ).replace('\1', '\1\1'
                      ).replace('\n', '\1\2')

    def load_row(row):
        return gpsdio.schema.import_msg(
            msgpack.loads(
                row.replace('\1\2', '\n'
                            ).replace('\1\1', '\1')))

    with gpsdio.open(infile) as i:
        with open(tempfile1, "w") as t:
            for line in i:
                key = getKey(line)
                t.write(key + " * " + format_row(line) + '\n')

    # Collate using C locale to sort by character value
    # See http://unix.stackexchange.com/questions/31886/how-do-get-unix-sort-to-sort-in-same-order-as-java-by-unicode-value/31922#31922
    # for infor on why this works for utf-8 text too

    env = dict(os.environ)
    env['LC_COLLATE'] = 'C' 

    subprocess.call(["sort", tempfile1, "-o", tempfile2], env=env)

    with open(tempfile2) as t:
        with gpsdio.open(outfile, "w") as o:
            for line in t:
                o.writerow(load_row(line.split(" * ", 1)[1][:-1]))

    os.unlink(tempfile1)
    os.unlink(tempfile2)

if __name__ == '__main__':
    gpsdio_sort()
