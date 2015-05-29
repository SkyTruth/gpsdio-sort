=====================
gpsdio sorting plugin
=====================


.. image:: https://travis-ci.org/SkyTruth/gpsdio-sort.svg?branch=master
    :target: https://travis-ci.org/SkyTruth/gpsdio-sort


.. image:: https://coveralls.io/repos/SkyTruth/gpsdio-sort/badge.svg?branch=master
    :target: https://coveralls.io/r/SkyTruth/gpsdio-sort


A CLI plugin for `gpsdio <https://github.com/skytruth/gpdsio/>`_ that sorts messages in arbitrarily large files according to an arbitrary set of columns.


Examples
--------

See ``gpsdio sort --help`` for info.

.. code-block:: console

    $ gpsdio sort input.msg output.msg \
        -c mmsi,timestamp


Installing
----------

Via pip:

.. code-block:: console

    $ pip install gpsdio-sort

From master:

.. code-block:: console

    $ git clone https://github.com/SkyTruth/gpsdio-sort
    $ cd gpsdio-sort
    $ pip install .


Developing
----------

.. code-block::

    $ git clone https://github.com/SkyTruth/gpsdio-sort
    $ cd gpsdio-sort
    $ virtualenv venv && source venv/bin/activate
    $ pip install -e .[test]
    $ py.test tests --cov gpsdio_sort --cov-report term-missing
