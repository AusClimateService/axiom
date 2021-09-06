"""CCAM DRS post-processing"""
import os
import argparse
import xarray as xr
import glob
import sys
import axiom.utilities as au
import axiom_schemas as axs
import numpy as np

from pprint import pprint
from calendar import monthrange
import pandas as pd
from datetime import datetime, timedelta
from uuid import uuid4
import re
from axiom.exceptions import ResolutionDetectionException


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

    # Load what was requested over top
    _meta.update(config[meta_key][key])

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


def preprocess_cordex(ds):
    """Preprocess the data upon loading for CORDEX requirments.

    Args:
        ds (xarray.Dataset): Dataset.

    Returns:
        xarray.Dataset: Dataset with preprocessing applied.
    """

    # Remove the first timestep, there is no data there
    ds = ds.isel(time=slice(1,None), drop=True)

    # Subtract 1min from the last time step, it steps over the boundary
    ds.time.data[-1] = ds.time.data[-1] - np.timedelta64(1, 'm')

    # Roll longitudes
    # ds = ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180))

    return ds


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
