"""Decorators."""
import functools


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

            if 'replace' in kwargs.keys() and kwargs['replace'] is True:
                result.attrs = meta
            else:
                result.attrs.update(meta)
            
            return result

        return __metadata
    
    return _metadata
    