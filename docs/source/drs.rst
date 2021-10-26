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


Python API
----------

The DRS functionality can be accessed via the Python API. For example:

.. code-block:: python

    import axiom.drs as drs
    import glob

    # Get a list of input files.
    input_files = sorted(glob.glob('/path/to/input/files/*.nc'))

    # Call the command
    drs.main(
        input_files,
        output_directory='/path/to/build/drs', # The full DRS structure will be built from here.
        start_year=2019, end_year=2019, # A single year
        output_frequency='1M', # Monthly frequency
        project='DELWP',
        model='ACCESS1-0',
        variable='tasmax',
        domains=['AUS-50'],
        cordex=True,
        input_resolution=None, # Auto-detect from input files.
        overwrite=True # Do not skip existing outputs, overwrite them.
    )