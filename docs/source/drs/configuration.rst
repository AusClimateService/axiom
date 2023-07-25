Configuration
=============

The Axiom DRS subsystem is based on a principal of cascading user configuration, whereby default configuration files are loaded from the Axiom installation's data directory, then OVERRIDDEN with a user-defined configuration file in the user's ``$HOME/.axiom`` folder.

- drs.json
- models.json
- projects.json
- domains.json

The user can copy all of these configuration files into their ``.axiom`` folder using the following command and modify them as required. This allows the user to modify configuration without impacting the default configuration.

.. code-block:: bash

    $ axiom drs_gen_user_config

This will backup any existing ``.axiom`` directory and reinstall from the default configuration.


drs.json
--------

The ``drs.json`` file contains all of the configuration options required to drive the DRS subsystem, it also defines some metadata defaults that are applied when building the metadata interpolation context used to create filepaths and metadata keys.

As there are a lot of keys and there is still rapid development underway it is not possible to describe every setting available in the drs.json file. Instead, users are encouraged to look at (or simply use) the ``drs.json`` file installed. Most keys are named to be fairly self-explanatory.

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

Users are encouraged to copy an existing model entry and modify it for their own purposes.

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

Users are encouraged to copy an existing project entry and modify it for their own purposes.

domains.json
------------

The domains.json file specifies keyed domain directives that are accessed through the CLI or Python API.

A domain is specified with a cell size, and a bounding box in degrees. The bounding box is specified as a minimum and maximum longitude and latitude. The cell size is specified in degrees.

.. code-block:: json

  {
    "AUS-11i": {
      "dx": 0.125,
      "lon_min": 88.75,
      "lon_max": 207.25,
      "lat_min": -53.25,
      "lat_max": 12.75
    }
  }

Users are encouraged to copy an existing domain entry and modify it for their own purposes.