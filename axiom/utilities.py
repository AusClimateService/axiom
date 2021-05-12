"""General utilities."""
import logging
import sys
from datetime import datetime
import pytz
from axiom.parsers import ParserFactory
import lxml.etree as le
import axiom.metadata as am
from axiom.validate import validate_xml
import axiom.conversion as axs
import xml.etree.ElementTree as et
from xml.dom import minidom


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


def _add_node(parent, child, text=None):
    _child = et.SubElement(parent, child)

    if text:
        _child.text = text

    return _child


def build_xml(root='metadata', **kwargs):
    """Build an xml document from a set of keywords.

    Args:
        root (str) : Name of the root node. Defaults to "metadata".
        **kwargs : Key/value pairs, where key is str and value can be str, datetime or dict.

    Returns:
        str : Formatted XML string for printing/writing.
    """

    # Create the root element
    _root = et.Element(root)

    for key, value in kwargs.items():

        # String
        if isinstance(value, str):
            _add_node(_root, key, value)

        # List
        if isinstance(value, list):
            for item in value:
                _add_node(_root, key, item)

        # Date
        if isinstance(value, datetime):
            _add_node(_root, key, value.strftime('%Y-%m-%dT%H:%M:%S'))

        # Dictionary
        if isinstance(value, dict):

            _parent = _add_node(_root, key)

            for _key, _value in value.items():
                _add_node(_parent, _key, _value)

    # Create XML
    raw_xml = et.tostring(_root, 'utf-8')

    # Make it pretty
    xml = minidom.parseString(raw_xml).toprettyxml(indent='    ')
    return xml
