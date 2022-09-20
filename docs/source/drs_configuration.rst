DRS Configuration
=================

The Axiom DRS subsystem is based on a principal of cascading user configuration, whereby default configuration files are loaded from the Axiom installation's data directory, then OVERRIDDEN with a user-defined configuration file in the user's $HOME/.axiom folder.

- drs.json
- models.json
- projects.json
- domains.json

drs.json
--------

The drs.json file contains all of the configuration options required to drive the DRS subsystem, it also defines some metadata defaults that are applied when building the metadata interpolation context used to create filepaths and metadata keys.

As there are a lot of keys and there is still rapid development underway it is not possible to describe every setting available in the drs.json file. Instead, users are encouraged to look at (or simply use) the drs.json file included in the axiom/data directory of the repository.

.. list-table::
   :widths: 10 10 40 40
   :header-rows: 1

   * - Key
     - Type
     - Description
     - Example
   * - time_units
     - string
     - Reference time units applied to outputs.
     - "days since 1949-12-01 00:00:00"
   * - reference_time
     - string
     - Reference time applied to outputs.
     - "1949-12-01 00:00:00"
   * - dask
     - dictionary
     - Settings to control the connection to dask.
     - See below.
   * - dask['enable']
     - boolean
     - Enable the dask interface
     - true
   * - dask['restart_client_between_variables']
     - boolean
     - Restart the client between variables.
     - true

models.json
-----------

The models.json file defines preliminary metadata that is used to build the interpolation context object used in filepaths and metadata. Top-level keys reflect the "model" argument that is passed to the DRS subsystem (either through the CLI or via the Python API) and point to a dictionary of otherwise arbitrary keys (NB: Arbitrary in the sense that nothing is inherently required unless explicitly used or interpolated elsewhere).

.. code-block:: json

    {
        "NCC-NorESM2-MM": {
            "model_lower": "noresm2-mm",
            "model_short": "norsesm2",
            "gcm_model": "NorESM2-MM",
            "gcm_institute": "NCC",
            "run_type": "Climate change",
            "mode": "bias- and variance-corrected sea surface temperatures",
            "description": "%(run_type)s run using %(gcm_institute)s-%(gcm_model)s %(experiment)s %(ensemble)s %(mode)s"
    }

See the axiom/data directory for a sample models.json file.

projects.json
-------------

The projects.json file defines preliminary metadata that is used to build the interpolation context object used in filepaths and metadata. Top-level keys reflect the "project" argument that is passed to the DRS subsystem (either through the CLI or via the Python API) and point to a dictionary of otherwise arbitrary keys (NB: Arbitrary in the sense that nothing is inherently required unless explicitly used or interpolated elsewhere).

.. code-block:: json

    {
        "CORDEX-CMIP6": {
            "base": "surf.ccam_%(res_km)skm",
            "project_lower": "acs",
            "rcp": "TBA",
            "experiment": "",
            "project_long": "2021 Climate and Resiliences Service Australia",
            "variables_2d": [
                "pr",
                "ps",
                "ts",
                "clh",
                "cll",
                "clm",
                "clt",
                "prc",
                "prw",
                "psl",
                "sic",
                "snc",
                "snd",
                "snm",
                "snw",
                "tas",
                "uas",
                "vas",
                "hfls",
                "hfss",
                "hurs",
                "huss",
                "mrro",
                "mrso",
                "orog",
                "prsn",
                "rlds",
                "rlut",
                "rsds",
                "rsdt",
                "rsus",
                "rsut",
                "sund",
                "tauu",
                "tauv",
                "zmla",
                "clivi",
                "clwvi",
                "mrfso",
                "mrros",
                "sftlf",
                "ta200",
                "ta500",
                "ta850",
                "ua200",
                "ua500",
                "ua850",
                "va200",
                "va500",
                "va850",
                "zg200",
                "zg500",
                "hus850",
                "prhmax",
                "tasmax",
                "tasmin",
                "evspsbl",
                "sfcWind",
                "evspsblpot",
                "sfcWindmax"
            ],
            "variables_3d": {},
            "variables_fixed": [
                "orog",
                "sftlf",
                "sftlaf",
                "srfurf",
                "sfturf"
            ]
        }
    }

See the axiom/data directory for a sample projects.json file.

domains.json
------------

The domains.json file specifies keyed domain directives that are accessed through the CLI or Python API.

See the axiom/data directory for details.