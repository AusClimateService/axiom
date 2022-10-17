"""General utilities."""
import logging
import sys
import json
import xml.etree.ElementTree as et
import pandas as pd
from datetime import datetime
import xarray as xr
import pkgutil
from collections import namedtuple
import glob
import os
from configparser import ConfigParser, ExtendedInterpolation
from pathlib import Path
import time
import subprocess as sp
import numpy as np
from jinja2 import Environment, BaseLoader


def dict2obj(d):
    """Convert a dictionary to an object."""
    obj = namedtuple("dict2obj", d.keys())(*d.values())
    return obj


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


def has_variable(ds, variable):
    """Check if a dataset contains a given variable.

    Args:
        ds (xarray.Dataset): Dataset.
        variable (str): Variable name.

    Returns:
        bool: True if ds contains variable, False otherwise.
    """
    return variable in list(ds.data_vars.keys())


def has_coord(dx, coord):
    """Check if a DataArray or Dataset contains a coordinate.

    Args:
        dx (xarray.DataArray or xarray.Dataset): Data.
        coord (str): Coordinate name.

    Returns:
        bool: True if coordinate in object, False otherwise.
    """
    return coord in list(dx.coords.keys())


def has_dim(dx, dim):
    """Check if a DataArray or Dataset contains a dimension.

    Args:
        dx (xarray.DataArray or xarray.Dataset): Data.
        dim (str): Dimension name.
    
    Returns:
        bool: True if dim on object, False otherwise.
    """
    return dim in list(dx.dims)


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
        ds (str or xarray.Dataset): Path to (netcdf) or dataset.

    Returns:
        dict : Metadata dictionary.
    """
    if isinstance(ds, str):
        ds = xr.open_dataset(ds)

    metadata = dict(
        _global=dict(),
        variables=dict()
    )

    # Add global attributes
    for key, value in ds.attrs.items():
        metadata['_global'][key] = infer_dtype(value)

    # Add variable attributes (includes coordinates)
    for v in get_variables_and_coordinates(ds):

        metadata['variables'][v] = dict()

        for key, value in ds[v].attrs.items():
            metadata['variables'][v][key] = infer_dtype(value)

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


def load_package_data(slug, package_name='axiom', return_type='json'):
    """Load data from the installed Axiom package.

    Args:
        slug (str): Internal data path.
        package_name (str) : Name of the package. Defaults to 'axiom'.

    Returns:
        mixed : Dictionary of data (JSON only ATM).
    """
    _raw = pkgutil.get_data(package_name, slug)

    # Allow return of raw data.
    if return_type == 'json':
        return json.loads(_raw.decode('utf-8'))
    
    if return_type == 'config':
        parser = ConfigParser()
        parser.read_string(_raw.decode('utf-8'))
        return parser

    if return_type == 'raw':
        return _raw

    raise Exception('Unknown return type.')


def apply_schema(ds, schema):
    """Apply a metadata schema on a dataset.

    Args:
        ds (xarray.Dataset): Dataset.
        schema (dict): Axiom schema dictionary.

    Returns:
        xarray.Dataset : Dataset with schema-defined metadata applied.
    """
    # Apply the global metadata, if available
    for key, _schema in schema['_global'].items():
        if 'allowed' in _schema.keys():
            ds.attrs[key] = _schema['allowed'][0]

    # Loop through each variable on the dataset
    for variable in list(ds.data_vars.keys()) + list(ds.coords.keys()):

        # If the variable is define
        if variable in schema['variables'].keys():

            # Extract the schema for the variable itself
            var_schema = schema['variables'][variable]

            # Loop through attributes
            for key, _schema in var_schema.items():

                # Apply if there is an expected value
                if 'allowed' in _schema.keys():
                    ds[variable].attrs[key] = _schema['allowed'][0]

    # Return the updated schema
    return ds

def _diff_metadata(meta_a, meta_b, ignore_matches=True):

    # Dictionaries are equal
    if meta_a == meta_b:
        diff = {key: (None, None) for key in meta_a.keys()}
        return diff

    # Otherwise we need to parse them
    diff = dict()

    # Check attributes from A
    parsed_keys = list()
    for key, value1 in meta_a.items():

        # Missing from B
        if key not in meta_b.keys():
            diff[key] = (value1, None)

        # Same value
        elif value1 == meta_b[key] and ignore_matches == False:
            diff[key] = (None, None)

        # Different value
        elif value1 != meta_b[key]:
            diff[key] = (value1, meta_b[key])

        # Mark as parsed for meta_b
        parsed_keys.append(key)

    # Check attributes of B
    for key, value2 in meta_b.items():

        # Skip already parsed
        if key in parsed_keys:
            continue

        # This will be the only test, the inverse has already been checked
        if key not in meta_a.keys():
            diff[key] = (None, value2)

    return diff


def diff_metadata(meta_a, meta_b, ignore_matches=True):
    """Difference the metadata between two metadata dictionaries.

    Differences are encoded with values as tuples, where:
    - (None, None) = Attributes match.
    - (None, value2) = Attribute is missing from meta_a.
    - (value1, None) = Attribute is missing from meta_b.
    - (value1, value2) = Differing values between meta_a and meta_b.

    Args:
        meta_a (dict): Metadata dictionary of the form from extract_metadata.
        meta_b (dict): Metadata dictionary of the form from extract_metadata.

    Returns:
        dict : Dictionary of differences.
    """

    # Do the global comparison
    diff = dict(
        _global=_diff_metadata(
            meta_a['_global'],
            meta_b['_global'],
            ignore_matches=ignore_matches
        ),
        variables=dict()
    )

    # Do the comparison of all the variables that the dicts share
    for variable in meta_a['variables'].keys():

        if variable in meta_b.keys():
            diff['variables'][variable] = _diff_metadata(
                meta_a[variable],
                meta_b[variable],
                ignore_matches=ignore_matches
            )

    return diff


def infer_dtype(value):
    """Infer the data type of the value passed.

    Args:
        value (unknown): Value.

    Raises:
        ValueError : When the type can't be inferred.
    """

    if isinstance(value, bool):
        return value

    # for dtype in [float, int, bool, str]:
    for dtype in [float, int, str]:

        try:
            return dtype(value)
        except ValueError:
            pass

    raise ValueError('Unable to infer type.')


def auto_glob(path):
    """Shorthand for sorted(glob.glob(path))

    Args:
        mask (str): Globabble path
    
    Returns:
        list : List of paths that match the globbable part.    
    """
    if not isinstance(path, list):
        return sorted(glob.glob(path))
    
    return path


def touch(filepath):
    """Thin wrapper for touching files.

    Args:
        filepath (str): Path.
    """
    Path(filepath).touch()


def get_lock_filepath(filepath):
    """Get a lock file path for filepath.

    Args:
        filepath (str): Path.

    Returns:
        str: Lock file path.
    """
    return f'{filepath}.lock'


def lock(filepath):
    """Place a lock on a filepath.

    Args:
        filepath (str): Path to the file.
    """
    touch(get_lock_filepath(filepath))


def unlock(filepath):
    """Remove a lock on a filepath.

    Args:
        filepath (str): Path to the file.
    """
    os.remove(get_lock_filepath(filepath))


def is_locked(filepath):
    """Check if a file is locked.

    Args:
        filepath (str): Path to the file.
    """
    return os.path.isfile(get_lock_filepath(filepath))


class ListAwareConfigParser(ConfigParser):

    def __init__(self):
        super().__init__(interpolation=ExtendedInterpolation())

    def getlist(self, section, key, delimiter='\n', cast=str):
        """Get an option out of the configuration and automatically split into a list.

        Args:
            section (str): Section name.
            key (str): Option key.
            delimiter (str, optional): Delimiter. Defaults to '\n'.
            cast (callable): Typecast on the fly to this type.

        Returns:
            list: Configuration option as a list.
        """
        values = self.get(section, key).split(delimiter)
        values = list(filter(bool, values))
        return [cast(v) for v in values]
    
    def section2dict(self, section, detect_dtypes=True):

        _dict = dict()
        for option in self._sections[section].keys():
            if detect_dtypes:
                _dict[option] = infer_dtype(self.get(section, option))
            else:
                _dict[option] = self.get(section, option)
        
        return _dict


def load_user_config(name):
    """Load a configuration file from the user's $HOME/.axiom directory.

    Args:
        name (str): Name of the configuration file.
    
    Returns:
        configparser.Config : Configuration.
    """
    # Build a path to the user's home directory for that configuration file.
    config_path = os.path.join(
        str(Path.home()),
        '.axiom',
        f'{name}.ini'
    )

    if os.path.isfile(config_path) is False:
        raise FileNotFoundError(f'No configuration file found at {config_path}')

    config = ListAwareConfigParser()
    config.read(config_path)
    return config


def pluralise(obj):
    """Automatically convert obj into a list object.

    For use when an object can be defined singularly or plurally.

    Args:
        obj (object): Object.
    """
    if not isinstance(obj, list):
        obj = [obj]
    
    return obj


class Timer:
    """Basic timer class."""

    def __init__(self):
        self._start_time = None
    
    def start(self):
        """Start the timer."""
        self._start_time = time.perf_counter()
    
    def stop(self):
        """Stop the timer.
        
        Returns:
            float : Elapsed time in seconds"""
        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        return elapsed_time


def shell(cmd, shell=True, check=True, capture_output=True, **kwargs):
    """Execute a shell command.

    Args:
        cmd (str): Command
        **kwargs : Passed to subprocess.run

    Returns:
        [type]: [description]
    """
    return sp.run(cmd, shell=shell, check=check, capture_output=capture_output, **kwargs)


def batch_split(iterable, n_batches):
    """Split iterable into n_batches.
    
    Args:
        iterable (iterable) : Iterable object to split.
        n_batches (int) : Number of batches.
    
    Returns:
        list : List of iterables.
    """
    return np.array_split(iterable, n_batches)


def conditional_rename(ds, **kwargs):
    """Conditionally rename an object on a dataset, if it exists.

    Args:
        ds (xarray.Dataset): Dataset.
        **kwargs : Key/value pairs of old=new names.

    Returns:
        xarray.Dataset : Dataset with renamed variables/coords.
    """
    # Start with an empty mapping.
    renames = dict()

    # If the old exists, then add it to the mapping.
    for old, new in kwargs.items():
        if in_ds(ds, old):
            renames[old] = new
    
    # If there are any mappings to apply, do them now
    if renames:
        return ds.rename(**renames)

    # Return the original
    return ds


def interpolate_template(template, **kwargs):
    """Interpolate onto a template string.
    
    Args:
        string (str): Template string.
        **kwargs : Key/value pairs to interpolate.
    
    Returns:
        str : Interpolated string
    """
    _template = Environment(loader=BaseLoader()).from_string(template)
    return _template.render(**kwargs)

