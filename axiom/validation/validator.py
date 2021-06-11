"""Validator class."""
import axiom.utilities as au
from cerberus import Validator as CerberusValidator
from datetime import datetime


class Validator:

    """Validation object:

    Args:
        schema (str, dict) : Path to schema or schema dictionary.
    """

    def __init__(self, schema):

        self.is_valid = None
        self.errors = dict(_global=list(), variables=dict())
        self.schema_filepath = None
        self.date_validated = None

        # Ensure schema is valid typew
        assert isinstance(schema, str) or isinstance(schema, dict)

        # Allow a path to be specified
        if isinstance(schema, str):
            self.schema_filepath = schema
            schema = au.load_schema_json(schema)

        self.schema = schema


    def validate(self, metadata, allow_unknown=True):
        """Validate metadata against a schema.

        Args:
            metadata ([type]): [description]
            allow_unknown (bool, optional): [description]. Defaults to True.

        Returns:
            bool : True if valid, False otherwise. Errors are stored in errors attribute.
        """

        # Reset the errors and valid status
        self.is_valid = True
        self.errors = dict(_global=list(), variables=dict())
        self.date_validated = datetime.utcnow()

        # Validate the global attributes
        v = CerberusValidator(schema=self.schema['_global'], allow_unknown=allow_unknown, require_all=True)
        if not v.validate(metadata['_global']):
            self.errors['_global'] = v._errors
            self.is_valid = False

        # Set up a default variable schema, to enfore a minimum standard
        if '_default' in self.schema['variables'].keys():
            default_schema = self.schema['variables']['_default']
        else:
            default_schema = dict()

        # Validate each variable one by one
        for k, attrs in metadata['variables'].items():

            # Apply the default schema, overwrite with the variable-specific one
            _schema = default_schema

            # Skip unknown variables if permitted
            if k not in self.schema['variables'].keys() and allow_unknown == False:
                continue

            # Apply this variable's schema to the default if it is defined
            if k in self.schema['variables'].keys():
                _schema.update(self.schema['variables'][k])

            v = CerberusValidator(
                schema=_schema,
                allow_unknown=allow_unknown,
                require_all=True
            )

            if not v.validate(attrs):
                self.errors['variables'][k] = v._errors
                self.is_valid = False

        return self.is_valid
