"""Entrypoint for the schemas subpackage."""
import os
import glob
import json
from importlib.metadata import version
from axiom.utilities import get_installed_data_root, get_user_data_root


# Order dependent, user to override system
SCHEMA_DIRS = [
    os.path.join(get_user_data_root(), 'schemas'),
    os.path.join(get_installed_data_root(), 'schemas')
]


def list_schema_filepaths():
    """List the schema filepaths.

    Returns:
        list: List of filepaths
    """
    schema_filepaths = list()
    # Get the installed schemas first, then the user schemas
    for schema_dir in SCHEMA_DIRS:
        if os.path.isdir(schema_dir):
            schema_filepaths += sorted(glob.glob(os.path.join(schema_dir, '*.json')))
    
    return schema_filepaths
    

def load_schemas():
    """Load all of the schemas into a dictionary object.

    Returns:
        dict : Dictionary of schemas
    """
    schemas = dict()
    for schema_filepath in list_schema_filepaths():
        schema = load_schema_file(schema_filepath)

        # Note, user schemas will override system schemas
        schemas[schema['name']] = schema
    
    return schemas


def load_schema(key_or_filepath):
    """Load the schema.

    Args:
        key_or_filepath (str): Schema key or filepath.
    
    Returns:
        dict : Schema dictionary.
    """

    # Load direct from the dictionary
    schemas = load_schemas()
    if key_or_filepath in schemas.keys():
        return schemas[key_or_filepath]

    # Attempt to load the file directly
    return load_schema_file(key_or_filepath)


def load_schema_file(filepath):
    """Actually load a schema filepath.

    Args:
        filepath (str): Path to the schema file.
    
    Returns:
        dict : Schema.
    """
    return json.loads(open(filepath, 'r').read())
    

def list_schemas():
    """List the available schemas (the json filepath).

    Returns:
        list : List of schema files available for loading.
    """
    # List everything in the schemas folder
    schemas = list_schema_filepaths()

    # Just return the basenames
    return [os.path.basename(s) for s in schemas]