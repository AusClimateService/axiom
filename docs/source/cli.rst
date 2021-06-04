.. _cli:
Command-line Usage
==================

Axiom is designed to be used on the command line as part of a larger workflow.

All entrypoints to the system are driven through the ``axiom`` command.

.. code-block:: shell

    axiom -h


Validation
------------

The ``validate`` subcommand will validate a data file against a defined metadata specification. By default, the command will return a non-zero exit status if the file does not meet the specification, however, a report can be generated in text format by adding the ``--report`` flag with a path at which to write the report.

.. code-block:: shell

    axiom validate -h

Example usage:

.. code-block:: shell

    axiom validate /path/to/specification.json /path/to/file.nc --report report.txt


Convert CF
----------

The ``convert_cf`` subcommand will convert the CF Conventions Standard Name Table (in XML format) into an Axiom schema.

.. code-block:: shell

    axiom convert_cf cf-standard-name-table.xml cf.json


Convert CORDEX
--------------

The ``convert_cordex`` subcommand will convert a CORDEX attribute CSV (from CCAM) into an Axiom schema.

.. code-block:: shell

    axiom convert_cordex codex_var_info_day.csv cordex-day.json