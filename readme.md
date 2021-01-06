# Axiom

A utility for validating/applying metadata templates for scientific data files.

## Installation

0. Set up a Python virtual/conda environment and activated (recommended).
1. Clone this repository, navigate into the cloned repository.
2. Run the command `pip install .` (including the dot).

## Command-line utilities

### Validator (axv)

A command-line utility to validate a data file against a known specification.

```shell
$ axv -h
usage: axv [-h] [-s {v1,latest}] input_path

Axiom Validator v0.1.0

positional arguments:
  input_path            Path to the file to validate.

optional arguments:
  -h, --help            show this help message and exit
  -s {v1,latest}, --specification {v1,latest}
                        Specification to validate against. Defaults to latest.
```

Example output:

```shell
$ axv test.nc
2021-01-06 13:52:05,278 - axiom.validate.validator - INFO - Axiom Validator v0.1.0
2021-01-06 13:52:05,316 - axiom.validate.validator - INFO - Validating /Users/sch576/work/axiom/test.nc
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Validating global attributes.
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Attribute: author
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Checking <class 'str'>
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Checking <function nonempty_str at 0x7ff1b5272790>
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Attribute: pwd
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Checking <class 'str'>
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Checking <function nonempty_str at 0x7ff1b5272790>
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Attribute: command
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Checking <class 'str'>
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Checking <function nonempty_str at 0x7ff1b5272790>
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Attribute: created
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Checking <class 'str'>
2021-01-06 13:52:05,316 - axiom.validate.validator - DEBUG - Checking <function parse at 0x7ff1b31b3160>
2021-01-06 13:52:05,318 - axiom.validate.validator - INFO - SUCCESS! File /Users/sch576/work/axiom/test.nc meets <axiom.validate.specification.V1 object at 0x7ff1b52707c0> specification.
```

### Editor (axe)

A command-line utility for applying a metadata specification to a data file.

(in progress)