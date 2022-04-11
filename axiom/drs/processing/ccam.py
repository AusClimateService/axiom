"""Pre and post-processing functions for CCAM."""
import numpy as np
import datetime
from calendar import monthrange

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


def preprocess_ccam(ds, variable=None):
    """Preprocess the data upon loading for CORDEX requirments.

    Args:
        ds (xarray.Dataset): Dataset.
        variable (str): Variable to extract along with bnds. Must be used as part of a lambda in open_mfdataset

    Returns:
        xarray.Dataset: Dataset with preprocessing applied.
    """

    if 'time' in list(ds.coords.keys()):

        # Remove the first timestep, there is no data there
        ds = ds.isel(time=slice(1,None), drop=True)

        # Subtract 1min from the last time step, it steps over the boundary
        ds.time.data[-1] = ds.time.data[-1] - np.timedelta64(1, 'm')

    # Rename metadata keys
    ds.attrs['rlon'] = ds.attrs.pop('rlong0')
    ds.attrs['rlat'] = ds.attrs.pop('rlat0')

    # Automatically detect version
    version = _detect_version(ds)
    ds.attrs['rcm_model'] = f'CCAM-{version}'
    ds.attrs['rcm_model_cordex'] = f'CCAM-{version}'
    ds.attrs['rcm_model_version'] = version
    ds.attrs['rcm_version'] = version
    ds.attrs['rcm_version_cordex'] = version


    # Extract the lat/lon bounds as well.
    if variable:
        ds = ds[[variable, 'lat_bnds', 'lon_bnds']]
    
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


def postprocess_ccam(ds):
    """For CORDEX processing, there is some minor postprocessing that happens.

    Args:
        ds (xarray.Dataset): Data.

    Returns:
        xarray.Dataset: Data with postprocessing applied.
    """

    # Check for time-invariance
    if 'time' not in list(ds.coords.keys()):
        return ds

    # Time coordinates need to be centered into the middle of the month
    centered_times = ds.time.to_pandas().apply(_center_date).values
    ds = ds.assign_coords(dict(time=centered_times))

    return ds