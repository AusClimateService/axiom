Validation
==========

Validating metadata is performed using either the command-line utility or using the Axiom Python library as part of your own Python code.

Using the Command Line
----------------------

See :ref:`Command Line Usage <cli>`

Using Python
------------

.. code-block:: python

    import axiom.validate as av
    import axiom.utilities as au
    import xarray as xr

    # Load the schema
    schema = au.load_schema_json('schema.json')

    # Create a validator
    v = av.Validator(schema=schema)
    # Note, you can also use av.Validator('schema.json') and save the above step.

    # Load metadata, multiple options...

    # Option 1 - metadata.json file
    metadata = au.load_metadata_json('metadata.json')

    # Option 2 - Extract from xarray.Dataset
    ds = xr.open_dataset('data.nc')
    metadata = au.extract_metadata(ds)

    # Validate the metadata against the schema (returns True/False)
    v.validate(metadata)

    # Or do something with the pass/failure
    if v.is_valid:
        print('Data meets specification.')
    else:

        # Errors are stored as an attribute
        for e in v.errors:
            # Do something with the error.
            pass


A report can also be generated for the validation instance, see :ref:`Reporting <reporting>`
