Anatomy of a Payload
====================

The payload consumption system of Axiom was developed in response to the overwhelming number of command-line arguments required to correctly configure a DRS processing instance. Rather than supplying a myriad of arguments, a user can supply a payload (JSON) file with an expected structure to initiate a DRS processing task. This approach has the added benefit of providing a mechanism for a decoupled workflow whereby a model simulation could write a payload file in a known location to be picked up periodically by another process, essentially acting as a simple message queue.

This page describes the fundamental structure of a payload file.

.. list-table:: 
   :widths: 25 25 50
   :header-rows: 1

   * - Key
     - Type
     - Description
   * - input_files
     - REQUIRED
     - A globbable path to input files for processing.
   * - output_directory
     - REQUIRED
     - Destination path from which DRS structure will be built.
   * - start_year
     - REQUIRED
     - First year to process.
   * - end_year
     - REQUIRED
     - Last year to process (set to same value as start_year to process 1 year).
   * - output_frequency
     - REQUIRED
     - Desired output frequency of output data, following the syntax of https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
   * - project
     - REQUIRED
     - Project key to read from projects.json for metadata.
   * - model
     - REQUIRED
     - Model key to read from models.json for metadata.
   * - domain
     - REQUIRED
     - Domain key to read from domains.json, or a domain directive.
   * - variables
     - REQUIRED
     - A list of variable names to process. An empty list will attempt to process all variables described by the schema references in drs.json.
   * - input_resolution
     - OPTIONAL
     - The input resolution in km of the input data. Leaving this blank will attempt to auto-detect the resolution from the input paths. It is best to provide the input resolution if known.

Any additional key/value pairs will be added to the processing context, which at the very least will be added to the output metadata, but may otherwise affect certain processing logic (usually in the case of custom pre/postprocessors) or interpolation templates described by drs.json.