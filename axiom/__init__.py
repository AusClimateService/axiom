"""Axiom main module."""
import xarray as xr
from importlib.metadata import version
import xmlschema as xs
import lxml.etree as et
import json
from dicttoxml import dicttoxml as _dict2xml
from xmltodict import parse as _xml2dict


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


def open_xsd(filepath):
    """Open an XML schema file.

    Args:
        filepath (str) : Path.

    Returns:
        xmlschema.XMLSchema : Schema object.
    """
    return xs.XMLSchema(filepath)


def open_xml(filepath):
    """Open an XML file.

    Args:
        filepath (str) : Path.

    Returns:
        lxml.etree.ElementTree : XML object
    """
    return et.parse(filepath)


def validate_xml(xml, xsd):
    """Validate an xml object against a schema.

    Args:
        xml (lxml.etree.ElementTree) : XML object from open_xml.
        xsd (xmlschema.XMLSchema) : Schema object from open_xsd.

    Raises:
        xmlschema.validators.exceptions.XMLSchemaChildrenValidationError : When validation fails.
    """
    xsd.validate(xml)


def validate2(xml, xsd, allow_unknown=True):

    # Get a list of the errors
    errors = xsd.iter_errors(xml)

    errors_parsed = list()

    # Filter unknown elements if requested
    for error in errors:

        # if 'Unexpected' in error.reason and allow_unknown is True:
        #     continue

        errors_parsed.append(error)

    errors = errors_parsed
    return errors


def xml2dict(xml):
    """Convert xml to dictionary.

    Args:
        xml (lxml.etree.ElementTree) : XML Object.

    Returns:
        dict : Dictionary.
    """

    # Convert to string
    xml_str = et.tostring(xml)

    # Convert to ordered dict
    od = _xml2dict(xml_str)

    # Convert to and from json to convert to standard dict and remove order.
    return json.loads(json.dumps(od))


def dict2xml(d, root='metadata'):
    """Convert a dictionary to an XML object.

    Args:
        d (dict) : Dictionary.
        root (str, Optional) : Root tag. Default to 'metadata'.

    Returns:
        lxml.etree.ElementTree : XML object.
    """
    # Convert to string, then parse
    xml_str = _dict2xml(d, custom_root=root, attr_type=False)
    return et.fromstring(xml_str)


def xr2dict(ds):
    """Convert an xarray.Dataset to a metadata dictionary.

    Args:
        ds (xarray.Dataset) : Dataset.

    Returns:
        dict : Dictionary of attributes.
    """
    return dict(metadata=ds.attrs)


def str2xml(s):
    """Convert an XML string into an XML object.

    Args:
        s (str) : String.

    Returns:
        lxml.etree.ElementTree : XML Object.
    """
    return et.fromstring(s)


def xml2str(xml, **kwargs):
    """Convert an xml object to a string.

    Args:
        xml (lxml.etree.ElementTree) : XML Object.
        **kwargs : Extra arguments to pass to lxml.etree.tostring()

    Returns:
        str : String representation of the XML document.
    """
    return et.tostring(xml, **kwargs)

def extract_metadata_xml(filepath):

    ds = xr.open_dataset(filepath)
    return dict2xml(ds.attrs)
