"""Axiom main module."""
import xarray as xr
from importlib.metadata import version


# Version handle
__version__ = version('axiom')


def load_data(filepath):
    """Load the data file.

    Args:
        filepath (str) : Path to the file.
    
    Returns:
        xarray.Dataset : Dataset.
    """
    ds = xr.open_dataset(filepath)
    return ds