"""CCAM DRS post-processing"""
import os
import re
import argparse
from pathlib import Path
import configparser as cp
import xarray as xr
import glob
import sys
import axiom.utilities as au
import axiom_schemas as axs
import numpy as np
import importlib
from pprint import pprint
from calendar import monthrange
import pandas as pd
from datetime import datetime, timedelta
from uuid import uuid4
import re
from axiom.exceptions import ResolutionDetectionException, MalformedDRSJSONPayloadException
from cerberus import Validator
from axiom.drs.domain import Domain
from axiom.config import load_config


def is_fixed_variable(config, variable):
    """Determine if a variable is listed in in the config as fixed.

    Args:
        config (dict): Configuration dictionary
        variable (str): Variable name.

    Returns:
        bool: True if fixed, False otherwise.
    """
    return variable in config['fixed_variables']


def get_template(config, key):
    """Get an interpolation template out of the config.

    Args:
        config (dict): Dictionary.
        key (str): Template key.

    Returns:
        str: Template.
    """
    # Get the template out of the configuration
    template = config['templates'][key]

    # Convert list to string (helps with really long templates)
    if isinstance(template, list):

        # The delimiter is the first item in the list
        template = template[0].join(template[1:])

    return template


def get_domains(resolution, frequency, variable_fixed, no_frequencies):
    """Get the domains for the arguments provided.

    Args:
        resolution (int): Resolution
        frequency (str): Frequency
        variable_fixed (bool): Is the variable fixed?
        no_frequencies (bool): True if no frequencies were provided.

    Raises:
        Exception: When the arguments provided don't yield any domain information.

    Returns:
        dict: Dictionary of domain information.
    """

    is_cordex = frequency == 'cordex'

    domains, frequencies, resolution_dir, degrees = None, None, None, None

    # Quickly bail if necessary
    assert resolution in [50, 5, 10, 12]

    if resolution == 50:

        degrees = 0.5
        resolution_dir = f'{resolution}km'

        if not is_cordex:
            domains = ['AUS-50']

        else:

            domains = ['AUS-44i']
            frequencies = ['1D', '1M']

            if variable_fixed:
                frequencies = ['1D']

        if no_frequencies:
            frequencies = ['3H', '1D', '1M']

    elif resolution == 5:

        domains = ['VIC-5']
        degrees = 0.05
        resolution_dir = f'{resolution}km'

    elif resolution == 10:

        domains = ['SEA-10', 'TAS-10']
        resolution_dir = '5km',
        degrees = 0.1

    elif resolution == 12:

        if not is_cordex:
            raise Exception('Domain not known.')

        domains = ['AUS-44i']
        frequencies = ['1D', '1M']

        if variable_fixed:
            frequencies = ['1D']

    # Return everything
    return dict(
        domains=domains,
        frequencies=frequencies,
        resolution_dir=resolution_dir,
        degrees=degrees
    )


def get_meta(config, meta_key, key):
    """Load metadata on top of a _default key, if applicable.

    Args:
        config (dict): Configuration dictionary.
        meta_key (str): Top-level key.
        key (str): Key.

    Returns:
        dict: Metadata dictionary, with key loaded over _default.
    """

    # Start with nothing
    _meta = dict()

    # Load a default, if it exists
    if '_default' in config[meta_key].keys():
        _meta.update(config[meta_key]['_default'])

    # Load what was requested over top, if it exists
    if key in config[meta_key].keys():
        _meta.update(config[meta_key][key])
    else:
        au.get_logger(__name__).warn(f'Key {key} is not present in {meta_key}, loading defaults only.')

    return _meta


def metadata(obj, **kwargs):
    """Add metadata to an xarray object.

    Args:
        da (xarray.DataArray or xarray.Dataset): xarray object.

    Returns:
        xarray.DataArray or xarray.Dataset : Same as caller
    """
    for key,value in kwargs.items():
        obj.attrs[key] = value

    return obj


def standardise_units(ds):
    """Standardise units.

    Only converts mm to m for now.

    Args:
        ds (xarray.Dataset): Dataset

    Returns:
        xarray.Dataset: Dataset with units standardised.
    """

    for variable in ds.data_vars.keys():
        da = ds[variable]

        # Convert mm to m
        if ds[variable].attrs['units'] == 'mm':
            ds[variable] = metadata(da / 1000, units='m')

    return ds


def detect_resolution(paths):
    """Attempt to detect the input resolution from a list of paths.

    Args:
        paths (list): List of file paths.

    Raises:
        ResolutionDetectionException: If there are too many possible resolutions in a path.
        ResolutionDetectionException: If there are inconsistent resolutions detected between paths.

    Returns:
        int: Resolution in km.
    """

    # Set up pattern for searching
    regex = r'([0-9.]*km)'
    res = None

    for path in paths:

        matches = re.findall(regex,  path)

        # Too many  options to choose from
        if len(matches) != 1:
            raise ResolutionDetectionException(f'Unable to detect resolution from {path}, there are too many possibilities.')

        # First detection
        if res is None:
            res = matches[0]

        # Already detected, but not the same
        elif res != matches[0]:
            raise ResolutionDetectionException(f'Detected resolutions are inconsistent between supplied paths.')

    # Made it this far, we have a detectable resolution
    return float(res.replace('km', ''))


def input_files_exist(paths):
    """Ensure all the input files actually exist.

    Args:
        paths (list): List of paths.

    Returns:
        bool: True if they all exist, False otherwise.
    """

    for path in paths:
        if not os.path.isfile(path):
            return False

    return True


def _center_date(dt):
    """Centre the date for compatibility with CDO-processed data.

    Args:
        dt (object): Date object.

    Returns:
        same as called: Date object with day set to middle of the month.
    """
    num_days = monthrange(dt.year, dt.month)[1]
    return dt.replace(day=num_days // 2)


def postprocess_cordex(ds):
    """For CORDEX processing, there is some minor postprocessing that happens.

    Args:
        ds (xarray.Dataset): Data.

    Returns:
        xarray.Dataset: Data with postprocessing applied.
    """

    # Time coordinates need to be centered into the middle of the month
    centered_times = ds.time.to_pandas().apply(_center_date).values
    ds = ds.assign_coords(dict(time=centered_times))

    return ds


def parse_domain_directive(directive):
    """Parse a domain directive (i.e. from CLI).

    Args:
        directive (str): Domain directive of the form name,dx,lat_min,lat_max,lon_min,lon_max
    
    Returns:
        dict : Domain dictionary
    
    Raises:
        ValueError : If the domain cannot be parsed.
    """
    name, *directives = directive.split(',')
    dx, lat_min, lat_max, lon_min, lon_max = map(float, directives)
    return dict(
        name=name,
        dx=dx,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max
    )


def parse_domain(directive):
    """Parse a domain directive.

    Domains are of the form: "name,fx,lat_min,lat_max,lon_min,lon_max"

    Args:
        directive (str) : Domain directive of the form name,dx,lat_min,lat_max,lon_min,lon_max.

    Returns:
        dict : Domain specification.

    Raises:
        AssertionError : When the directive is missing componenents.
        TypeError : When the directive is unable to be parsed.
    """
    segments = directive.split(',')
    assert len(segments) == 6
    name = segments[0]
    dx, lat_min, lat_max, lon_min, lon_max = [float(s) for s in segments[1:]]

    # Return a dictionary of the parsed information.
    return dict(
        name=name,
        dx=dx,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max
    )


def load_domain_config():
    """Load domain configuration out of installed data dir.

    Returns:
        configparser.Config: Configuration object.
    """
    return load_config('domains')


def get_domain(key):
    """Load a domain out of the internal configuration.

    Args:
        key (str): Domain key or parseable domain directive.
    
    Returns:
        axiom.domain.Domain : Domain object.
    """

    # Load the domain configuration as a configuration object.
    domain_config = load_domain_config()
    return Domain.from_config(key, domain_config)


def is_registered_domain(key):
    """Quick shortcut to see if the domain is already registered in the system.

    Args:
        key (str): Domain key.

    Returns:
        bool: True if registered, False otherwise.
    """
    return key in load_domain_config().keys()


def load_processor(model_key, proc_type='pre'):
    """Load a pre-or-post processor for the model, if one exists.

    Args:
        model_key (str): Model.

    Returns:
        callable: Function that takes an xarray.Dataset as input.
    """
    logger = au.get_logger(__name__)

    try:
        mod = importlib.import_module(f'axiom.drs.processing.{model_key}')
        processor = getattr(mod, f'{proc_type}process_{model_key}')
        logger.info(f'Found {proc_type}processor for {model_key}')
        return processor
    except:
        logger.warning(f'No {proc_type}processor found for {model_key}, returning empty function.')
        return lambda ds, *args, **kwargs: ds

def load_preprocessor(model_key):
    """Shorthand for the load_processor function.

    Args:
        model_key (str): Model
    """
    return load_processor(model_key=model_key, proc_type='pre')


def load_postprocessor(model_key):
    """Shorthand for the load_processor function.

    Args:
        model_key (str): Model
    """
    return load_processor(model_key=model_key, proc_type='post')



def interpolate_context(context):
    """Interpolate the context dictionary into itself, filling all placeholders.

    Args:
        context (dict): Context dictionary.
    
    Returns:
        dict : Interpolated context.
    """
    logger = au.get_logger(__name__)
    for key, value in context.items():
        # context[key] = value % context
        new_value = str(value) % context
        logger.debug(f'{key} = {new_value}')
        context[key] = new_value
    
    return context


def get_uninterpolated_placeholders(string):
    """Check if a string has any remaining uninterpolated values.

    Args:
        string (string): String object.
    
    Returns:
        list : List of uninterpolated values.
    """
    # Regex to find matches
    matches = re.findall(r'%\(([a-zA-Z0-9_-]+)\)s', string)

    # Convert to set to remove duplicates, convert back and return
    return sorted(list(set(matches)))


def detect_input_frequency(ds):
    """Detect the (closest) input frequency of the data.

    Args:
        ds (xarray.Dataset): Input data.
    
    Returns:
        str : Pandas offset directive.
    """

    # Bail out if time-invariant
    if is_time_invariant(ds):
        return 'fx'

    # Take the difference between the first two time steps
    total_seconds = (ds.time.data[1] - ds.time.data[0]).astype('timedelta64[s]').astype(np.int32)

    # TODO: These should be configurable
    intervals = '1H,3H,6H,1D,1M'.split(',')
    seconds = [
        1*60*60,
        3*60*60,
        6*60*60,
        24*60*60,
        28*24*60*60
    ]

    # Take the closest
    ix = np.argmin(np.abs(np.array(seconds) - total_seconds))
    return intervals[ix]


def is_time_invariant(ds):
    """Test if the dataset is time-invariant (has no time coordinate)

    Args:
        ds (xarray.Dataset or xarray.DataArray): Data
    
    Return:
        bool : True if no 'time' coordinate detected, False otherwise
    """
    return 'time' not in list(ds.coords.keys())