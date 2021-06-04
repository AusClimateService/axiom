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
        self.is_valid = None
        self.errors = dict(_global=list(), variables=dict())
        self.date_validated = datetime.utcnow()

        # Validate the global attributes
        v = CerberusValidator(schema=self.schema['_global'], allow_unknown=allow_unknown, require_all=True)
        if not v.validate(metadata['_global']):
            self.errors['_global'] = v._errors
            self.is_valid = False

        # Validate each variable one by one
        for k, attrs in metadata['variables'].items():

            # Skip unknown variables if permitted
            if k not in self.schema['variables'].keys() and allow_unknown:
                continue

            v = CerberusValidator(
                schema=self.schema['variables'][k],
                allow_unknown=allow_unknown,
                require_all=True
            )

            if not v.validate(attrs):
                self.errors['variables'][k] = v._errors
                self.is_valid = False

        return self.is_valid