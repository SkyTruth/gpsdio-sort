#!/usr/bin/env python


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
    '-c', '--cols', metavar='COL,[COL,...]', default="timestamp",
    help="Sort rows by columns.  Specify as a comma separated list.  (default: timestamp)",
)
@click.argument("infile")
@click.argument("outfile")
@click.pass_context
def gpsdio_sort(ctx, infile, outfile, cols='timestamp'):

    """
    Sort messages by column in large files.

    Sorts messages in an arbitrarily large file according to an arbitrary set
    of columns, by default 'timestamp'.

    The unix `sort` command is used so large tempfiles may be written.
    """

    # Make sure unix sort is available
    for p in os.environ['PATH'].split(os.pathsep):
        if os.access(os.path.join(p, 'sort'), os.X_OK):
            break
    else:
        raise click.ClickException("Unix sort is not on path.")

    # Figure out if we can get the driver and compression out of the
    # parent click context
    if isinstance(ctx.obj, dict):
        i_drv = ctx.obj.get('i_drv')
        i_cmp = ctx.obj.get('i_cmp')
        o_drv = ctx.obj.get('o_drv')
        o_cmp = ctx.obj.get('o_cmp')
    else:
        i_drv = None
        i_cmp = None
        o_drv = None
        o_cmp = None

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
        ).replace('\1', '\1\1').replace('\n', '\1\2')

    def load_row(row):
        return gpsdio.schema.import_msg(
            msgpack.loads(
                row.replace('\1\2', '\n').replace('\1\1', '\1')))

    with gpsdio.open(infile, driver=i_drv, compression=i_cmp) as i:
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
        with gpsdio.open(outfile, "w", driver=o_drv, compression=o_cmp) as o:
            for line in t:
                o.writerow(load_row(line.split(" * ", 1)[1][:-1]))

    os.unlink(tempfile1)
    os.unlink(tempfile2)

if __name__ == '__main__':
    gpsdio_sort()
