"""General utilities."""
import logging
import sys


def get_logger(name, level='debug'):
    """Get a logging object.

    Args:
        name (str): Name of the module currently logging.
        level (str, optional): Level of logging to emit. Defaults to 'debug'.

    Returns:
        logging.Logger: Logging object.
    """

    logger = logging.Logger(name)
    handler = logging.StreamHandler(sys.stdout)
    level = getattr(logging, level.upper())
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_variables_and_coordinates(ds):
    """Get a list of variable and coordinate names.

    Args:
        ds (xarray.Dataset): Dataset

    Returns:
        list: List of variable and coordinate names
    """
    return list(ds.data_vars.keys()) + list(ds.coords.keys())


def in_ds(variable, ds):
    """Test if variable is in the data file.

    Args:
        variable (str): Variable name.
        ds (xarray.Dataset): Data.
    
    Returns:
        bool : True if the variable exists, False otherwise.
    """
    return variable in get_variables_and_coordinates(ds)


def has_attr(obj, attr):
    """Test if obj has the attribute attr.

    Args:
        obj (xarray.DataArray or xarray.Dataset): xarray object.
        attr (str): Name of the attribute.

    Returns:
        bool: True if obj has attribute attr, False otherwise.
    """
    return attr in obj.attrs.keys()
