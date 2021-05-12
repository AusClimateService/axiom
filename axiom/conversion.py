from dict2xml import dict2xml as _dict2xml
from xmltodict import parse as _xml2dict


def dict2xml(d, root):
    """Convert a dictionary to XML.

    Args:
        d (dict) : Dictionary object.
        root (str) : Root node.

    Returns:
        str : XML string version of dict.
    """
    return _dict2xml(d, root=root)


def xml2dict(xml):
    """Convert an XML string to a dictionary.

    Args:
        xml (str) : XML string

    Returns:
        dict : Dictionary representation of the XML string.
    """
    return _xml2dict(xml)
