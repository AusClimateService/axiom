"""General utilities."""
import logging
from sys import version
import json
import xml.etree.ElementTree as et
import pandas as pd
from datetime import datetime


def get_logger(name, level='debug'):
    """Get a logging object.

    Args:
        name (str): Name of the module currently logging.
        level (str, optional): Level of logging to emit. Defaults to 'debug'.

    Returns:
        logging.Logger: Logging object.
    """

    logger = logging.Logger(name)
    handler = logging.StreamHandler(sys.stdout)
    level = getattr(logging, level.upper())
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_variables_and_coordinates(ds):
    """Get a list of variable and coordinate names.

    Args:
        ds (xarray.Dataset): Dataset

    Returns:
        list: List of variable and coordinate names
    """
    return list(ds.data_vars.keys()) + list(ds.coords.keys())


def in_ds(variable, ds):
    """Test if variable is in the data file.

    Args:
        variable (str): Variable name.
        ds (xarray.Dataset): Data.

    Returns:
        bool : True if the variable exists, False otherwise.
    """
    return variable in get_variables_and_coordinates(ds)


def has_attr(obj, attr):
    """Test if obj has the attribute attr.

    Args:
        obj (xarray.DataArray or xarray.Dataset): xarray object.
        attr (str): Name of the attribute.

    Returns:
        bool: True if obj has attribute attr, False otherwise.
    """
    return attr in obj.attrs.keys()


def extract_metadata(ds):
    """Extract metadata from an xarray dataset.

    Args:
        ds (xarray.Dataset): Dataset.
    
    Returns:
        dict : Metadata dictionary.
    """
    # Add global attributes
    metadata = dict(
        _global=ds.attrs,
        variables=dict()
    )

    # Add variable attributes (includes coordinates)
    for v in get_variables_and_coordinates(ds):
        metadata['variables'][v] = ds[v].attrs
    
    return metadata


def load_metadata_json(filepath):
    """Load a metadata.json file.

    Args:
        filepath (str): Path to the json file.

    Returns:
        dict: Metadata dictionary.
    """
    metadata = json.loads(open(filepath, 'r').read())
    return metadata


def load_schema_json(filepath):
    """Load a schema.json file.

    Args:
        filepath (str): Path to the json file.
    
    Returns:
        dict : Schema dictionary.
    """
    return load_metadata_json(filepath)


def load_cf_standard_name_table(filepath):
    """Load and convert a CF standard name table XML into a metadata schema.

    Args:
        filepath (str): Path to the file.
    
    Returns:
        dict : Metadata dictionary (will be long).
    """
    xml = et.parse(filepath)
    root = xml.getroot()

    # Set up the schema information
    schema = dict(
        name='CF Standard Names Table',
        description='Specification converted from the CF standard names table',
        original_file=filepath.split('/')[-1],
        variables=dict()
    )

    # Header mappings
    header_mapping = dict(
        version_number='version',
        institution='contact',
        contact='contact_email',
        last_modified='created'
    )

    # Variable mappings
    child_mapping = dict(
        canonical_units='units',
        grib='grib',
        amip='amip',
        description='description'
    )

    # Iterate through the children
    for child in root:

        # Map the header information
        if child.tag not in ['entry', 'alias']:
            schema[header_mapping[child.tag]] = child.text

        # Variable information
        if child.tag == 'entry':

            # Get the name of the variable
            key = child.attrib['id']

            _schema = dict(
                standard_name={
                    'type': 'string',
                    'allowed': [key]
                }
            )

            # Iterate through the required metadata attributes
            for _child in child:

                _key = child_mapping[_child.tag]

                _schema[_key] = {
                    'type': 'string'
                }

                # Allow for nullable fields
                if _child.text:
                    _schema[_key]['allowed'] = [_child.text]
            
            # Add variable schema to the main schema
            schema['variables'][key] = _schema
        
        # Copy aliases
        if child.tag == 'alias':
            other = child.find('entry_id').text
            schema['variables'][child.attrib['id']] = schema['variables'][other]

    return schema


def save_schema(schema, filepath):
    """Save the schema to a json file at filepath.

    Args:
        schema (dict): Schema dictionary.
        filepath (str): Path to which to save the schema.
    """
    json.dump(schema, open(filepath, 'w'), indent=4)


def get_timestamp():
    """Generate a JSON-compliant timestamp.

    Returns:
        str : Timestamp.
    """
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')


def load_cordex_csv(filepath, **kwargs):
    """Load a cordex csv (i.e. from CCAM) and convert it into a metadata schema.

    Note: There are no _global attributes.

    Args:
        filepath (str): Path to file
        **kwargs : Additional key/value pairs to add to the schema header (i.e. contact etc.)
    
    Returns:
        dict : Schema dictionary
    """
    # Load the CSV verbatim
    df = pd.read_csv(filepath)

    schema = dict(
        name='CORDEX Metadata Specification',
        description='Converted from CSV',
        original_file=filepath.split('/')[-1],
        created=get_timestamp(),
        **kwargs,
        _global=dict()
    )

    # Convert the lines into metadata specifications
    variables = dict()

    # Loop through variables
    for line in df.to_dict('records'):

        key = line['variable']
        variables[key] = dict()

        # Loop through attributes
        for attribute, value in line.items():

            # Skip variable, we already have it
            if attribute == 'variable':
                continue
        
            variables[key][attribute] = {
                'type': 'string',
            }

            # Check if there is an expected value
            if pd.isna(value) == False:
                variables[key][attribute]['allowed'] = [value]

    schema['variables'] = variables
    return schema