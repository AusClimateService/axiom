# Axiom

*An established rule or principle, a self-evident truth*

Axiom is a prototype utility for validating/applying metadata templates for scientific data files.

It works on the principles of XML schema validation () whereby metadata is extracted from scientific data, converted to XML and validated against a required standard (schema).

## Installation

0. Set up a Python virtual/conda environment and activated (recommended).
1. Clone this repository, navigate into the cloned repository.
2. Run the command `pip install .` (including the dot).

## Command-line utilities

### Validate

The validation utility is used to evaluate whether a data file has the required metadata to meet the standards specified in a schema file.

```
$ axiom validate -h
usage: axiom validate [-h] [-r REPORT_FILEPATH] schema_filepath input_filepath

Validate an input file against a schema.

positional arguments:
  schema_filepath       Path to schema file.
  input_filepath        File to validate

optional arguments:
  -h, --help            show this help message and exit
  -r REPORT_FILEPATH, --report_filepath REPORT_FILEPATH
                        Path to write validation report.
```

Example usage (Spread across multiple lines for clarity):

```
$ SCHEMA=specifications/mrd.xsd
$ DATA_FILE=/path/to/data.nc
axiom validate $SCHEMA $DATA_FILE -r report.txt
```

This will return a nonzero exit code if the data provided fails to match the schema. A report can optionally be generated, which takes the following form to advise the user how to "fix" their metadata to match the requirements of the schema.

```cat report.txt
Axiom Validator 0.1.0
Report generated: 2021-05-14 04:57:46.729151
schema_filepath: specifications/mrd.xsd
input_filepath: /path/to/data.nc
Status: FAIL
+-----------------------+------------------------------------------------------+
| Key                   | Error                                                |
+=======================+======================================================+
| author                | Required key author is missing.                      |
+-----------------------+------------------------------------------------------+
| owner                 | Required key owner is missing.                       |
+-----------------------+------------------------------------------------------+
| version               | Required key version is missing.                     |
+-----------------------+------------------------------------------------------+
| created               | Required key created is missing.                     |
+-----------------------+------------------------------------------------------+
| license               | Required key license is missing.                     |
+-----------------------+------------------------------------------------------+
| citation              | Required key citation is missing.                    |
+-----------------------+------------------------------------------------------+
| history               | Value of history does not match schema requirements. |
+-----------------------+------------------------------------------------------+
| resolution_horizontal | Required key resolution_horizontal is missing.       |
+-----------------------+------------------------------------------------------+
| resolution_vertical   | Required key resolution_vertical is missing.         |
+-----------------------+------------------------------------------------------+
| resolution_temporal   | Required key resolution_temporal is missing.         |
+-----------------------+------------------------------------------------------+
| averaging_horizontal  | Required key averaging_horizontal is missing.        |
+-----------------------+------------------------------------------------------+
| averaging_vertical    | Required key averaging_vertical is missing.          |
+-----------------------+------------------------------------------------------+
| averaging_temporal    | Required key averaging_temporal is missing.          |
+-----------------------+------------------------------------------------------+
```
