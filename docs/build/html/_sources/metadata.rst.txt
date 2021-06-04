.. _metadata:
Metadata
========

Metadata can be provided to Axiom in three different ways:

1. Loading a metadata JSON format.
2. Extracting metadata from a NetCDF file.
3. Programmatically at runtime.


Loading a metadata JSON
--------------------------

.. code-block:: json

    {
        "_global": {
            "author": "John Smith",
            "description": "Scientific outputs."
        },
        "variables":{
            "t2": {"units": "K"},
            "u10": {"units": "m/s"}
        }
    }


Then:

.. code-block:: python

    import axiom.utilities as au
    metadata = au.load_metadata_json('metadata.json')


Extracted from an Xarray dataset
-----------------------------------

.. code-block:: python

    import xarray as xr
    import axiom.utilities as au

    ds = xr.open_dataset('data.nc')
    metadata = au.extract_metadata(ds)


Defined programmatically at runtime
--------------------------------------

.. code-block:: python

    metadata = dict(
        _global=dict(
            author='John Smith',
            description='Scientific outputs.'
        ),
        variables=dict(
            t2=dict(units='K'),
            u10=dict(units='m/s')
        )
    )


The three examples above are equivalent.

For metadata to be useful, it needs to be validated against a :ref:`Schema <schemas>`.