Metadata Schemas
================

A metadata schema (also referred to as a "specification") is a configuration file which defines the rules and standards that metadata must conform to in order to "meet the standard" and pass validation checks. They too are written as a JSON configuration file following a strict format and using validation rules defined by the Cerberus validation library, a subsystem used by Axiom.

https://docs.python-cerberus.org/en/stable/index.html

A metadata schema follows a format as follows:

.. code-block:: json

    {
        "name": "Name of the specification",
        "version: "0.1.0",
        "description": "A description for the specification.",
        "contact": "The person who can be contacted regarding the specification.",
        "contact_email": "contact@example.com",
        "_global": {},
        "variables": {}
    }

The main points of the schema are listed under the "_global" and "variables" keys, which are expanded as Cerberus validation dictionaries, however, the other items are required by Axiom to properly process the schema. Additional entries in the header are allowed but ignored by Axiom.

Global attributes
~~~~~~~~~~~~~~~~~

Global attributes are listed in the "_global" key of the schema JSON file, with all child keys evaluated using the Cerberus validation subsystem. The format is a key-value pair of attribute names (key) and attribute rules (value), for example:

.. code-block:: json

    {
        "_global": {
            "author": {"type": "string"},
            "description": {"type": "string"},
            "date_created": {"type": "datetime"}
        }
    }

Validation rules can be found at https://docs.python-cerberus.org/en/stable/validation-rules.html. Note: all global metadata keys are required by default, so there is no need to add ``"required": true`` to the validation rules for a given metadata key. If an existing key is not required or can take multiple forms, consider either omitting it from the specification, or updating the specification to enforce a standard. The latter is preferable.

Variable attributes
~~~~~~~~~~~~~~~~~~~

Variable attributes are defined in much the same way as global attributes, with the exception of being nested under their variable name.

For example:

.. code-block:: json

    {
        "variables": {
            "t2": {
                "units": {"type": "string", "allowed": ["K", "C"]},
                "description": {"type": "string"}
            }
        }
    }

Again, all attributes defined in a variable's schema are required by default.

Default variable attribute
--------------------------

There is one special configuration option for variable schemas, the ``_default`` configuration option. If a schema provides a ``_default`` configuration option, it sets a base set of validation rules which all other variables inherit or override.

For example:

.. code-block:: json

    {
        "variables": {
            "_default": {
                "units" : {"type": "string"},
                "description" : {"type": "string"},
                "standard_name": {"type": "string"},
                "long_name": {"type": "string"}
            }
        }
    }


This example will enforce all variables to require units, description, standard_name and long_name as metadata attributes, unless they provide their own set of rules.

Putting it all together
~~~~~~~~~~~~~~~~~~~~~~~

Using the above examples, the complete metadata schema could take the following form:

.. code-block:: json

    {
        "name": "My specification",
        "version: "0.1.0",
        "description": "A simple specification.",
        "contact": "John Smith",
        "contact_email": "john.smith@example.com",
        "_global": {
            "author": {"type": "string"},
            "description": {"type": "string"},
            "date_created": {"type": "datetime"}
        },
        "variables": {
            "t2": {
                "units": {"type": "string", "allowed": ["K", "C"]},
                "description": {"type": "string"}
            }
        }
    }