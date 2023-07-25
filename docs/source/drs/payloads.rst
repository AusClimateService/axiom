Payloads
========

The payload consumption system of Axiom was developed in response to the overwhelming number of command-line arguments required to correctly configure a DRS processing instance. Rather than supplying a myriad of arguments, a user can supply a payload (JSON) file with an expected structure to initiate a DRS processing task. This approach has the added benefit of providing a mechanism for a decoupled workflow whereby a model simulation could write a payload file in a known location to be picked up periodically by another process, essentially acting as a simple message queue.

Generating Payloads
-------------------

The simplest way to generate a valid payload is to create one programmatically using the Python API.

.. code-block:: python

  from drs.payload import Payload

  # Create a payload object
  payload = Payload(

    # Specify a globbable path to the input files.
    input_files='/path/to/files/*.nc',

    # Specify the output directory (DRS structure will be built from here).
    output_dir='/path/to/output/',

    # Specify the model, project and domain keys to read from configuration.
    model='MODEL',
    project='PROJECT',
    domain='DOMAIN',

    # Specify the start and end years to process (these are usually the same).
    start_year=2000,
    end_year=2000,

    # Specify the variable names to process
    # This is optional, omitting will load the expected variables from the schema
    variables=['tasmax', 'tasmin'],

    # Specify the output frequency of the data (i.e. 1D, 6H, 1D or 1M)
    output_frequency='1D',

    # Any further keywords will be added to the processing context
    # as additional metadata.
    experiment='historical',
    ensemble='r1i1p1f1'
    # ... and so on
  )

  # Write the payload to a file
  payload.to_json('/path/to/payload.json')

The resulting JSON file will be suitable for ``drs_consume`` to process.

Generating LOTS of payloads
---------------------------

Using this approach, it is trivial to generate large number of payloads quickly. For example, to generate a payload for every year of a 100 year simulation, you could do the following:

.. code-block:: python

  # Example from above

  # Write the payload to a file
  for year in range(2000, 2101):
    payload.start_year = year
    payload.end_year = year
    payload.to_json(f'/path/to/payloads/payload_{year}.json')


Consuming Payloads
------------------

In order to use the payloads to run a DRS processing instance, Axiom must "consume" the payloads. This is done using the ``drs_consume`` command-line tool.

.. code-block:: bash

  $ axiom drs_consume /path/to/payloads/*.json

This will run a DRS processing instance of each payload in the supplied glob. Note: this will consume each payload file in sequence in the execution environment. Typically, users have large volumes of data which is better processed through the orchestration of HPC resources. In this case, it is recommended to use the ``drs_launch`` command-line tool to submit a job to a HPC scheduler (see below).


Launching Payloads
------------------

The ``drs_launch`` command-line tool is used to submit a DRS processing instance to a HPC scheduler. This tool is designed to be used in conjunction with the ``drs_consume`` tool to submit a job for each payload in a glob.

.. code-block:: bash

  $ axiom drs_launch --help
  usage: axiom drs_launch [-h] [-d] [-i] [--walltime WALLTIME] [--unlock] path jobscript log_dir

  Submit drs_consume tasks via qsub.

  positional arguments:
    path                 Globbable path to payload files (use quotes)
    jobscript            Path to the jobscript for submission.
    log_dir              Directory to which to write logs.

  options:
    -h, --help           show this help message and exit
    -d, --dry_run        Print commands without executing.
    -i, --interactive    Dump the interactive flag into the qsub command when dry-running.
    --walltime WALLTIME  Override walltime in job script.
    --unlock             Unlock locked payloads prior to submission

Most of this command is best explained through the ``--help`` flag, however, the user is expected to supply a jobscript file relevant to their HPC environment. An example jobscript is provided below:

.. code-block:: bash

  #!/bin/bash
  #PBS -l walltime=12:00:00
  #PBS -l ncpus=48
  #PBS -l mem=190G
  #PBS -q normal
  #PBS -l storage=gdata/abc
  #PBS -l jobfs=400G
  #PBS -j oe
  #PBS -l wd

  set -ex

  # Load the conda environment
  source ~/.bashrc
  conda activate axiom_dev

  # Run the consume command
  axiom drs_consume $AXIOM_PAYLOAD >> $AXIOM_LOG_DIR/$PBS_JOBNAME.log

  # Check if any variables failed in processing by looking for the .failed file
  failed_filepath="${AXIOM_PAYLOAD}.failed"
  if [ -f "$failed_filepath" ]; then
      echo "$failed_filepath exists. Some variables have failed to process."
      exit 1
  else
      echo "$failed_filepath does not exist. All variables have processed successfully."
      exit 0
  fi

In this instance, the script will have available to it the following environment variables:

- ``$AXIOM_PAYLOAD``: The path to the payload file to be consumed.
- ``$AXIOM_LOG_DIR``: The path to the directory in which to write logs.

If your HPC system requires it, the ``-l storage`` flag will likely be required to include the location of the input files, the output destination, and the location of your Axiom installation.

Anything else can be added to the jobscript as required by your specific environment.