Metadata Conversion
===================

To compensate for the variety of metadata specification formats available, Axiom provides a number of conversion utilities to convert these formats into something it can use.

CF Conventions
--------------

The CF Conventions standard name table is available as an XML download from the following URL:

https://cfconventions.org/standard-names.html

Axiom does not use this file directly, rather it converts the file into a metadata schema for use in data validation.

.. code-block:: python

    import axiom.utilities as au
    schema = au.load_cf_standard_name_table('/path/to/file.xml')

Note: this schema will be very big (~130K lines formatted) but remains human/machine readable.


CORDEX CSV
----------

Development efforts for CCAM have extracted metadata requirements manually from specification PDF files into a CSV format. Axiom provides a utility to convert this format into a schema for interoperability:

.. code-block:: python

    import axiom.utilities as au
    schema = au.load_cordex_csv(
        '/path/to/file.csv',
        contact='John Smith',
        contact_email='john.smith@example.com',
        version='0.1.0'
    )


Notes:

- Additional keyword arguments provided to the function are added to the schema header.
- No global metadata is applied during this function (as there is none to infer); hence, the user is encouraged to add these manually.

.. code-block:: python

    schema['_global'] = dict(
        author='John Smith',
        description='Scientific outputs.'
        # etc.
    ) 


Saving Converted Schemas
------------------------

All schemas, converted or otherwise can be saved using the ``save_schema`` method.

.. code-block:: python

    au.save_schema(schema, 'my-schema-0.1.0.json')

It is good practice to add version information into the filepath.