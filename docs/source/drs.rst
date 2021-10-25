DRS
===

Axiom has inbuilt functionality to convert CCAM outputs into Data Reference Syntax (DRS).

Command-line interface
----------------------

.. code-block:: shell

    axiom drs -h
    usage: axiom drs [ARGS] input_files [input_files ...] output_directory

    DRS utility

    positional arguments:
    input_files           Input filepaths
    output_directory      Output base directory (DRS structure built from here)

    optional arguments:
    -h, --help            show this help message and exit
    -o, --overwrite       Overwrite existing output
    -s START_YEAR, --start_year START_YEAR
                            Start year
    -e END_YEAR, --end_year END_YEAR
                            End year
    -r INPUT_RESOLUTION, --input_resolution INPUT_RESOLUTION
                            Input resolution in km, leave blank to auto-detect
                            from path.
    -f [output_frequency ...], --output_frequency [output_frequency ...]
                            Output frequency, Examples include "12min", "1M" (1
                            month) etc. see https://pandas.pydata.org/pandas-
                            docs/stable/user_guide/timeseries.html#offset-aliases.
    -p {DELWP,WINE,ACS,_default,_default_12min}, --project {DELWP,WINE,ACS,_default,_default_12min}
    -m {ERA,ERA-NUDGED,ERA5,ACCESS1-0,CCSM4,CNRM-CM5,GFDL-ESM2M,HadGEM2,MIROC5,MPI-ESM-LR,NorESM1-M}, --model {ERA,ERA-NUDGED,ERA5,ACCESS1-0,CCSM4,CNRM-CM5,GFDL-ESM2M,HadGEM2,MIROC5,MPI-ESM-LR,NorESM1-M}
    -d [domain ...], --domain [domain ...]
                            Domains to process, space-separated.
    -v [variable ...], --variable [variable ...]
                            Variables to process, omit to use those defined in
                            config.
    --cordex              Process for CORDEX