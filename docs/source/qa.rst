Quality Assurance
=================

Axiom features a simple quality assurance (QA) module for testing data integrity.

Checking timeseries continuity
------------------------------

.. code-block:: shell

    axiom qa-timeseries -h
    usage: axiom qa-timeseries [-h] [--errors] path schema start_year end_year output_filepath

    positional arguments:
        path             Globbable path with {variable} and {year} placeholders (use quotes).
        schema           Schema name or path from which to load variables.
        start_year       Start year.
        end_year         End year.
        output_filepath  Output filepath for report.

    options:
        -h, --help       show this help message and exit
        --errors         Output only errors.


Example usage:

.. code-block:: shell

    axiom qa-timeseries '/g/data/.../v1/1hr/{variable}/{variable}*_{year}*.nc' CORDEX 1980 2020 qa-1hr.csv --errors