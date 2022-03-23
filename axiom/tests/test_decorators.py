"""Tests for decorator methods."""
from axiom.decorators import metadata
import xarray as xr


def test_metadata():
    """Test the metadata decorator, which adds metadata to the return value of a function."""
    data = [1, 2, 3, 4]
    da = xr.DataArray(data)

    @metadata(name='temp', units='K')
    def _func(da):
        return da
    
    da = _func(da)

    assert da.attrs['units'] == 'K' and da.name == 'temp'