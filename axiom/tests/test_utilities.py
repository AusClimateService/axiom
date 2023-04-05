"""Test utilities."""
import axiom.utilities as au
import xarray as xr
import numpy as np


def test_isolate_coordinate():
    """Test the isolate_coordinate function."""

    # Create an object with time, lat, lon
    obj = xr.DataArray(
        data=np.arange(54).reshape(6, 3, 3),
        coords={'time': [1, 2, 3, 4, 5, 6], 'lat': [1, 2, 3], 'lon': [1, 2, 3]},
        dims=['time', 'lat', 'lon'],
    )

    # Isolate the lat coordinate
    result = list(au.isolate_coordinate(obj, 'lat', drop=True).coords)

    # Ensure we get only one and that is the one we want.
    assert len(result) == 1
    assert result[0] == 'lat'