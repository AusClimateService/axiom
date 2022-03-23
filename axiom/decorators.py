"""Decorators."""
import functools
import xarray as xr


def metadata(**meta):
    """Apply metadata to the xarray outputs of a function.

    Usage Example:

    @metadata(units='K')
    def get_temp(ds):
        return ds.T

    """
    def _metadata(func):
        @functools.wraps(func)
        def __metadata(*args, **kwargs):
            result = func(*args, **kwargs)

            # Allow name to be set on DataArrays, special edge case.
            if 'name' in meta.keys() and isinstance(result, xr.DataArray):
                result.name = meta.pop('name')

            # Allow the user to completely replace any existing metadata
            if 'replace' in meta.keys() and meta['replace'] is True:
                meta.pop('replace')
                result.attrs = meta
            else:
                result.attrs.update(meta)
            
            return result

        return __metadata
    
    return _metadata
    