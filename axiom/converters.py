"""Conversion routines to convert between units automatically. EXPERIMENTAL."""
from axiom.decorators import metadata


@metadata(units='m')
def mm2m(da):
    """Convert mm to m.

    Args:
        da (xarray.DataArray): Data in mm.

    Returns:
        xarray.DataArray: Data in m.
    """
    return da / 1000.0

conversion_mappings = {
    'mm:m': mm2m
}

def ensure_units(da, expected_units):
    """Ensure that a given DataArray has the correct units, converting if needed.

    Args:
        da (xarray.DataArray): Data.
        expected_units (ste): Units value.

    Returns:
        xarray.DataArray: Data with units converted.
    
    Raises:
        KeyError : When no unit attribute is present.
        KeyError : When no converter is registered between the old and new units.
        Exception: When the converter fails to convert the units.
    """
    
    # Get the current units information
    current_units = da.attrs['units']
    
    # No conversion required
    if current_units == expected_units:
        return da

    # Lookup the conversion routine
    converter = conversion_mappings[f'{current_units}:{expected_units}']

    # Call it
    return converter(da)
    