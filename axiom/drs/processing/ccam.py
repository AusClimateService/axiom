"""Pre and post-processing functions for CCAM."""
import numpy as np
import sys
import datetime
from calendar import monthrange
import axiom.drs.utilities as adu
import axiom.utilities as au


def _detect_version(ds):
    """The CCAM version can be detected from the history metadata.

    Args:
        ds (xarray.Dataset): Dataset
    
    Returns:
        str : Version.
    """
    history = ds.attrs['history']
    yymm = datetime.datetime.strptime(history.split()[2], '%Y-%m-%d').strftime('%y%m')
    return yymm


def _set_version_metadata(ds, version):
    """Set the version metadata on the DataSet.

    Args:
        ds (xarray.DataSet): Dataset.
        version (str): Version string.

    Returns:
        xarray.Dataset : Dataset with version metadata on it.
    """
    ds.attrs['rcm_model'] = f'CCAM-{version}'
    ds.attrs['rcm_model_cordex'] = f'CCAM-{version}'
    ds.attrs['rcm_model_version'] = version
    ds.attrs['rcm_version'] = version
    ds.attrs['rcm_version_cordex'] = version
    return ds


def preprocess_ccam(ds, **kwargs):
    """Preprocess the data upon loading for CORDEX requirments.

    Args:
        ds (xarray.Dataset): Dataset.
        variable (str): Variable to extract along with bnds. Must be used as part of a lambda in open_mfdataset

    Returns:
        xarray.Dataset: Dataset with preprocessing applied.
    """

    variable = kwargs['variable']

    # Rename metadata keys
    ds.attrs['rlon'] = ds.attrs.pop('rlong0')
    ds.attrs['rlat'] = ds.attrs.pop('rlat0')

    # Automatically detect version from inputs
    if 'model_id' not in kwargs['kwargs'].keys():
        version = _detect_version(ds)
    else:
        version = kwargs['kwargs']['model_id'].split('-')[-1]
        
    ds = _set_version_metadata(ds, version)
        
    # Extract the lat/lon bounds as well.
    if variable:
        ds = ds[[variable, 'lat_bnds', 'lon_bnds']]

    return ds

def center_times(ds, output_frequency):
    """Centers the times in the dataset.

    Args:
        ds (xarray.Dataset): Data.
    
    Returns:
        xarray.Dataset : Data with times centered.
    """

    # non-monthly data is simple, just halve the delta
    if output_frequency != '1M':
        dt = ds.time.data[1] - ds.time.data[0]
        ds['time'] = ds.time + (dt / 2)
        return ds

    # Otherwise, we need to apply more logic to the problem.
    dt = ds.time.data[1:] - ds.time.data[0:-1]
    new_times = ds.time.data[:] + (dt / 2)
    new_times = np.append(new_times, new_times[6]) # july
    ds['time'] = new_times


def postprocess_ccam(ds, **kwargs):
    """For CORDEX processing, there is some minor postprocessing that happens.

    Args:
        ds (xarray.Dataset): Data.

    Returns:
        xarray.Dataset: Data with postprocessing applied.
    """

    logger = au.get_logger(__name__)

    # Strip out the extra dimensions from bnds (reduces filesize considerably)
    if 'lat_bnds' in ds.data_vars.keys():

        if adu.is_time_invariant(ds):

            ds['lat_bnds'] = ds.lat_bnds.isel(lon=0, drop=True)
            ds['lon_bnds'] = ds.lon_bnds.isel(lat=0, drop=True)
            return ds

        else:

            ds['lat_bnds'] = ds.lat_bnds.isel(lon=0, time=0, drop=True)
            ds['lon_bnds'] = ds.lon_bnds.isel(lat=0, time=0, drop=True)

    # Center the times for non-instantaneous data.
    _is_instantaneous = is_instantaneous(ds, kwargs['variable'])
    _resampling_applied = kwargs['resampling_applied']
    
    logger.debug(f'is_instantaneous = {_is_instantaneous}')
    logger.debug(f'resampling_applied = {_resampling_applied}')
    if _resampling_applied == True:
        logger.debug('TIME CENTERING TRIGGERED')
        ds = center_times(ds, output_frequency=['output_frequency'])
    
    return ds


def is_instantaneous(ds, variable):
    """Checks for the presence of CCAM-specific flags indicating that a variable is instantaneous.

    Args:
        ds (xarray.Dataset): Data.
        variable (str): Variable currently being processed.
    """

    da = ds[variable]

    # if cell_methods is missing
    if 'cell_methods' not in da.attrs.keys():
        return True
    
    # time: point is present
    if da.attrs['cell_methods'] == 'time: point':
        return True
    
    return False
