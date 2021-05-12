"""Methods for working with metadata."""
import axiom.utilities as au
from dict2xml import dict2xml
import lxml.etree as le


def apply_metadata_template(ds, template, replace=False):
    """Apply a standard set of metadata to the dataset.

    Args:
        ds (xarray.Dataset) : Dataset.
        template (dict) : Metadata template.
        replace (bool) : Replace existing metadata, defaults to False.

    Returns:
        xarray.Dataset : Data with metadata applied.
    """

    # Apply metadata to each of the coordinates and variables
    for variable in ax.get_variables_and_coordinates(ds):
        if variable in template['variables']:

            if replace:
                ds[variable].attrs = template['variables'][variable]
            else:
                ds[variable].attrs.update(template['variables'][variable])

    # Apply to the dataset as a whole
    if replace:
        ds.attrs = template['global']
    else:
        ds.attrs.update(template['global'])

    return ds


def ds2xml(ds):
    """Convert a dataset to xml metadata.

    Args:
        ds (xarray.Dataset) : Dataset

    Returns:
        str : xml string
    """
    xml = dict2xml(ds.attrs)
    return xml
