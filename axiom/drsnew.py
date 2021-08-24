"""CCAM DRS post-processing"""
import os
import argparse
import xarray as xr
import glob
import sys
import axiom.utilities as au
import axiom_schemas as axs
import numpy as np

from pprint import pprint
from calendar import monthrange
import pandas as pd
from datetime import datetime, timedelta
from uuid import uuid4
import re
from axiom.exceptions import ResolutionDetectionException


def is_fixed_variable(config, variable):
    """Determine if a variable is listed in in the config as fixed.

    Args:
        config (dict): Configuration dictionary
        variable (str): Variable name.

    Returns:
        bool: True if fixed, False otherwise.
    """
    return variable in config['fixed_variables']


def get_template(config, key):
    """Get an interpolation template out of the config.

    Args:
        config (dict): Dictionary.
        key (str): Template key.

    Returns:
        str: Template.
    """
    # Get the template out of the configuration
    template = config['templates'][key]

    # Convert list to string (helps with really long templates)
    if isinstance(template, list):

        # The delimiter is the first item in the list
        template = template[0].join(template[1:])

    return template

def get_domains(resolution, frequency, variable_fixed, no_frequencies):
    """Get the domains for the arguments provided.

    Args:
        resolution (int): Resolution
        frequency (str): Frequency
        variable_fixed (bool): Is the variable fixed?
        no_frequencies (bool): True if no frequencies were provided.

    Raises:
        Exception: When the arguments provided don't yield any domain information.

    Returns:
        dict: Dictionary of domain information.
    """

    is_cordex = frequency == 'cordex'

    domains, frequencies, resolution_dir, degrees = None, None, None, None

    # Quickly bail if necessary
    assert resolution in [50, 5, 10, 12]

    if resolution == 50:

        degrees = 0.5
        resolution_dir = f'{resolution}km'

        if not is_cordex:
            domains = ['AUS-50']

        else:

            domains = ['AUS-44i']
            frequencies = ['1D', '1M']

            if variable_fixed:
                frequencies = ['1D']

        if no_frequencies:
            frequencies = ['3H', '1D', '1M']

    elif resolution == 5:

        domains = ['VIC-5']
        degrees = 0.05
        resolution_dir = f'{resolution}km'

    elif resolution == 10:

        domains = ['SEA-10', 'TAS-10']
        resolution_dir = '5km',
        degrees = 0.1

    elif resolution == 12:

        if not is_cordex:
            raise Exception('Domain not known.')

        domains = ['AUS-44i']
        frequencies = ['1D', '1M']

        if variable_fixed:
            frequencies = ['1D']

    # Return everything
    return dict(
        domains=domains,
        frequencies=frequencies,
        resolution_dir=resolution_dir,
        degrees=degrees
    )


def get_meta(config, meta_key, key):
    """Load metadata on top of a _default key, if applicable.

    Args:
        config (dict): Configuration dictionary.
        meta_key (str): Top-level key.
        key (str): Key.

    Returns:
        dict: Metadata dictionary, with key loaded over _default.
    """

    # Start with nothing
    _meta = dict()

    # Load a default, if it exists
    if '_default' in config[meta_key].keys():
        _meta.update(config[meta_key]['_default'])

    # Load what was requested over top
    _meta.update(config[meta_key][key])

    return _meta


def metadata(obj, **kwargs):
    """Add metadata to an xarray object.

    Args:
        da (xarray.DataArray or xarray.Dataset): xarray object.

    Returns:
        xarray.DataArray or xarray.Dataset : Same as caller
    """
    for key,value in kwargs.items():
        obj.attrs[key] = value

    return obj


def standardise_units(ds):
    """Standardise units.

    Only converts mm to m for now.

    Args:
        ds (xarray.Dataset): Dataset

    Returns:
        xarray.Dataset: Dataset with units standardised.
    """

    for variable in ds.data_vars.keys():
        da = ds[variable]

        # Convert mm to m
        if ds[variable].attrs['units'] == 'mm':
            ds[variable] = metadata(da / 1000, units='m')

    return ds


def detect_resolution(paths):
    """Attempt to detect the input resolution from a list of paths.

    Args:
        paths (list): List of file paths.

    Raises:
        ResolutionDetectionException: If there are too many possible resolutions in a path.
        ResolutionDetectionException: If there are inconsistent resolutions detected between paths.

    Returns:
        int: Resolution in km.
    """

    # Set up pattern for searching
    regex = r'([0-9.]*km)'
    res = None

    for path in paths:

        matches = re.findall(regex,  path)

        # Too many  options to choose from
        if len(matches) != 1:
            raise ResolutionDetectionException(f'Unable to detect resolution from {path}, there are too many possibilities.')

        # First detection
        if res is None:
            res = matches[0]

        # Already detected, but not the same
        elif res != matches[0]:
            raise ResolutionDetectionException(f'Detected resolutions are inconsistent between supplied paths.')

    # Made it this far, we have a detectable resolution
    return float(res.replace('km', ''))


def input_files_exist(paths):
    """Ensure all the input files actually exist.

    Args:
        paths (list): List of paths.

    Returns:
        bool: True if they all exist, False otherwise.
    """

    for path in paths:
        if not os.path.isfile(path):
            return False

    return True


def preprocess_cordex(ds):
    """Preprocess the data upon loading for CORDEX requirments.

    Args:
        ds (xarray.Dataset): Dataset.

    Returns:
        xarray.Dataset: Dataset with preprocessing applied.
    """

    # Remove the first timestep, there is no data there
    ds = ds.isel(time=slice(1,None), drop=True)

    # Subtract 1min from the last time step, it steps over the boundary
    ds.time.data[-1] = ds.time.data[-1] - np.timedelta64(1, 'm')

    # Roll longitudes
    # ds = ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180))

    return ds


def _center_date(dt):
    """Centre the date for compatibility with CDO-processed data.

    Args:
        dt (object): Date object.

    Returns:
        same as called: Date object with day set to middle of the month.
    """
    num_days = monthrange(dt.year, dt.month)[1]
    return dt.replace(day=num_days // 2)


def postprocess_cordex(ds):
    """For CORDEX processing, there is some minor postprocessing that happens.

    Args:
        ds (xarray.Dataset): Data.

    Returns:
        xarray.Dataset: Data with postprocessing applied.
    """

    # Time coordinates need to be centered into the middle of the month
    centered_times = ds.time.to_pandas().apply(_center_date).values
    ds = ds.assign_coords(dict(time=centered_times))

    return ds


def parse_args(config):
    """Parse arguments for command line utiltities.

    Returns:
        argparse.Namespace : Arguments object.
    """

    # Pull some config
    VALID_PROJECTS = config['projects'].keys()
    VALID_MODELS = config['models'].keys()
    VALID_DOMAINS = config['domains'].keys()

    # Build a parser
    parser = argparse.ArgumentParser()
    parser.description = "DRS utility"

    # Paths
    parser.add_argument('input_files', type=str, help='Input filepaths', nargs="+")
    parser.add_argument('output_directory', type=str, help='Output base directory (DRS structure built from here')
    parser.add_argument('-o', '--overwrite', default=False, help='Overwrite existing output', action='store_true')

    # Temporal
    parser.add_argument('-s', '--start_year', required=True, type=int, help='Start year')
    parser.add_argument('-e','--end_year', required=True, type=int, help='End year')

    # Resolution and output frequency
    parser.add_argument('-r', '--input_resolution', type=float, help='Input resolution in km, leave blank to auto-detect from path.')
    parser.add_argument(
        '-f', '--output_frequency', required=True, type=str,
        nargs='*', metavar='output_frequency',
        help='Output frequency, Examples include "12min", "1M" (1 month) etc. see https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases.'
        )

    # Metadata
    parser.add_argument('-p', '--project', required=True, type=str, choices=VALID_PROJECTS)
    parser.add_argument('-m', '--model', required=True, type=str, choices=VALID_MODELS)

    # Domains, we can process multiple at once
    parser.add_argument(
        '-d', '--domain',
        required=True, type=str,
        choices=VALID_DOMAINS,
        nargs='*', metavar='domain',
        help='Domains to process, space-separated.'
    )


    # Override the variables defined in the drs.json file
    parser.add_argument(
        '-v', '--variable',
        type=str,
        nargs='*',
        metavar='variable',
        help='Variables to process, omit to use those defined in config.'
    )

    # Switch on CORDEX options
    parser.add_argument(
        '--cordex',
        help='Process for CORDEX',
        action='store_true',
        default=False
    )

    return parser.parse_args()


if __name__ == '__main__':

    logger = au.get_logger(__name__)

    logger.debug('Loading config')
    config = au.load_package_data('data/drs.json')

    # Parse arguments
    args = parse_args(config)

    # Detect the input resolution if it it not supplied
    if args.input_resolution is None:
        logger.debug('No input resolution supplied, auto-detecting')
        args.input_resolution = detect_resolution(args.input_files)
        logger.debug(f'Input resolution detected as {args.input_resolution} km')

    # Test that the input files exist
    logger.debug('Ensuring all input files actually exist.')
    assert input_files_exist(args.input_files)

    # Load project, model and domain metadata
    logger.debug(f'Loading project ({args.project}) and model ({args.model}) metadata.')
    project = get_meta(config, 'projects', args.project)
    model = get_meta(config, 'models', args.model)

    # Establish the filename base template (this will need to be interpolated)
    filename_base = project['base']

    if args.variable:

        logger.debug(f'User has supplied variables ({args.variable}).')
        variables = {v: list() for v in args.variable}

    else:

        # Load variables to be processed (2d and 3d)
        logger.debug('No variables supplied, loading from config.')
        variables_2d = project['variables_2d']
        variables_3d = project['variables_3d']

        # Create a dictionary of variables to process keyed to an empty list of levels for 2D
        variables = {v2d: list() for v2d in variables_2d}

        # Add in the 3D variables, with levels this time
        for v3d, levels in variables_3d.items():
            variables[v3d] = levels

        num_variables = len(variables)
        logger.debug(f'{num_variables} variables to process.')

    # Test that the output directory exists, create if not
    logger.debug(f'Creating {args.output_directory}')
    os.makedirs(args.output_directory, exist_ok=True)

    # Loop through the years between start and end in decades
    logger.debug('Constructing a list of years to process')
    years = range(args.start_year, args.end_year+1, 10)

    # Assemble the context object (order dependent!)
    logger.debug('Assembling interpolation context.')
    context = config['defaults'].copy()
    context.update(project)
    context.update(model)
    context.update(vars(args))

    # Add rcm metadata
    if args.cordex:
        logger.debug('User has requested CORDEX processing, adding rcm metadata')
        context['rcm_version'] = context['rcm_version_cordex']
        context['rcm_model'] = context['rcm_model_cordex']

    # Open all the files
    logger.debug('Loading files into distributed memory, this may take some time.')

    # Remove the first timestep from each cordex monthly file
    if args.cordex:
        logger.debug('Preprocessing cordex inputs.')
        dss = xr.open_mfdataset(args.input_files, chunks=dict(time=1), preprocess=preprocess_cordex)
    else:
        dss = xr.open_mfdataset(args.input_files, chunks=dict(time=1))

    logger.debug('Standardising units')
    dss = standardise_units(dss)

    # Subset the variables requested
    logger.debug('Extracting variables %s' % list(variables.keys()))
    dss = dss[list(variables.keys())]

    # Select the levels for any 3D variables in the dataset
    logger.debug('Subsetting levels')
    for variable, levels in variables.items():
        for level in levels:
            dss[f'{variable}{level}'] = dss.sel(lev=level, drop=True)

    # Sort the dimensions (fixes domain subsetting)
    logger.debug('Sorting data')
    dss = dss.sortby(['time', 'lat', 'lon'])

    print(dss)

    # sys.exit()

    # Work out which schema to use based on output frequency
    logger.debug('Applying metadata schema')
    if 'M' in args.output_frequency:
        schema = axs.load_schema('cordex-month.json')
    else:
        schema = axs.load_schema('cordex-day.json')

    # print(schema)
    # sys.exit()
    # dss = au.apply_schema(dss, schema)

    # Process each year
    logger.debug('Starting processing')

    # Loop through each year
    for year in years:

        # Work out the end year based on some hard-coded rules
        start_year = year
        end_year = start_year + 9

        # TODO: Need a cleaner way to do this
        if start_year < 2006:
            context['experiment'] = 'historical'
        else:
            context['experiment'] = context['rcp']

        if context['gcm_model'] in ['ERAINT', 'ERA5']:
            context['experiment'] = 'evaluation'
            description_template = 'description_era'
        else:
            description_template = 'description_other'

        context['description'] = get_template(config, description_template) % context

        if start_year == 2006:
            end_year = 2009

        # Non-ERA
        elif start_year == 2000 and model['model_lower'][:3] != 'era':
            end_year = 2005

        # ERA
        elif start_year == 2000 and model['model_lower'][:3] != 'era':
            end_year = 2014

        start_date = f'{start_year}0101'
        end_date = f'{end_year}1231'

        context['start_date'] = start_date
        context['end_date'] = end_date

        logger.debug(f'start_date = {start_date}')
        logger.debug(f'end_date = {end_date}')

        logger.debug(f'Processing {year}')

        # Loop through each output frequency
        for output_frequency in args.output_frequency:

            logger.debug(f'Processing {output_frequency} frequency')

            logger.debug('Resampling')
            dss_f = dss.resample(time=output_frequency).mean()

            for domain in args.domain:

                logger.debug(f'Processing {domain}')
                context['domain'] = domain

                _domain = config['domains'][domain]

                # Fix to cross the meridian
                lon_min, lon_max = _domain['lon_min'], _domain['lon_max']
                if lon_max < lon_min:
                    constraint = (dss_f.lon <= lon_min) | (dss_f.lon >= lon_max)

                else:
                    constraint = (dss_f.lon >= lon_min) & (dss_f.lon <= lon_max)

                dss_d = dss_f.where(constraint, drop=True)

                # Subset the domain
                # logger.debug('Subsetting domain.')
                # lon_constraint = (dss_d.lon >= _domain['lon_min']) & (dss_d.lon <= _domain['lon_max'])
                # lat_constraint = (dss_d.lat >= _domain['lat_min']) & (dss_d.lat <= _domain['lat_max'])
                #
                # # lon_slice = slice(_domain['lon_min'], _domain['lon_max'])
                # # lat_slice = slice(_domain['lat_min'], _domain['lat_max'])
                # # dss_d = dss_f.sel(lon=lon_slice, lat=lat_slice)
                #
                # # Have to subset the domain to account for antimeridian coordinates
                # dss_d = dss_d.sel(lon=lon_constraint, lat=lat_constraint)
                # print(dss_d)
                # sys.exit()

                logger.debug('Starting metadata assembly')

                logger.debug('Setting description')
                if model['model_lower'][:3] == 'era':
                    context['description'] = config['templates']['description_era'] % context
                else:
                    context['description'] = config['templates']['description_other'] % context

                # Map the frequency to something DRS-compliant
                context['frequency_mapping'] = config['frequency_mapping'][output_frequency]

                # Tracking info
                context['created'] = datetime.utcnow()
                context['uuid'] = uuid4()

                logger.debug('Assembling global metadata')
                global_attrs = dict()

                for key, value in config['defaults'].items():
                    global_attrs[key] = value % context

                # Add in meta that we'd like to retain, renaming it in the process
                logger.debug('Retaining metadata from inputs')
                for old_key, new_key in config['retain_metadata'].items():

                    if old_key in dss.attrs.keys():
                        global_attrs[new_key] = dss.attrs[old_key]

                # Remove any metadata that is not needed in the output
                logger.debug('Removing requested metadata from inputs')
                for rm in config['remove_metadata']:
                    global_attrs.pop(rm)

                # Strip and reapply metadata
                logger.debug('Applying metadata')
                dss_d.attrs = dict()
                dss_d.attrs = global_attrs

                # Loop through variables and write out data
                logger.debug('Starting variable processing')
                for variable in dss_d.data_vars.keys():

                    logger.debug(f'Processing {variable}')

                    # Update the context
                    context['variable'] = variable

                    # Get the full output filepath with string interpolation
                    logger.debug('Working out output paths')
                    output_dir = get_template(config, 'drs_path') % context
                    output_filename = get_template(
                        config, 'filename') % context
                    output_filepath = os.path.join(output_dir, output_filename)
                    logger.debug(f'output_filepath = {output_filepath}')

                    # Skip if already there and overwrite is not set, otherwise continue
                    if os.path.isfile(output_filepath) and args.overwrite == False:
                        logger.debug(f'{output_filepath} exists and overwrite is set to False, skipping.')
                        continue

                    # Create the output directory
                    logger.debug(f'Creating {output_dir}')
                    os.makedirs(output_dir, exist_ok=True)

                    # Assemble the encoding dictionaries (to ensure time units work!)
                    logger.debug('Applying encoding')
                    encoding = config['encoding'].copy()
                    encoding[variable] = encoding.pop('variables')

                    # Center the months etc.
                    dss_d = postprocess_cordex(dss_d)

                    # Nested list selection creates a degenerate dataset for per-variable files
                    logger.debug(f'Writing {output_filepath}')
                    dss_d[[variable]].to_netcdf(
                        output_filepath,
                        format='NETCDF4_CLASSIC',
                        encoding=encoding,
                        unlimited_dims=['time']
                    )


        # # Work out the end year based on some hard-coded rules
        # start_year = year
        # end_year = start_year + 9

        # if start_year == 2006:
        #     end_year = 2009

        # # Non-ERA
        # elif start_year == 2000 and model['model_lower'][:3] != 'era':
        #     end_year = 2005

        # # ERA
        # elif start_year == 2000 and model['model_lower'][:3] != 'era':
        #     end_year = 2014

        # start_date = f'{start_year}0101'
        # end_date = f'{end_year}1231'

        # context['start_date'] = start_date
        # context['end_date'] = end_date

        # logger.debug(f'start_date = {start_date}')
        # logger.debug(f'end_date = {end_date}')

        # sys.exit()

        # sys.exit()


        # # Loop through each frequency
        # for frequency in domains['frequencies']:

        #     # Perform the resampling to get the frequency required
        #     dss_f = dss.resample(time=frequency).mean()

        #     # Loop through each domain
        #     for domain in domains['domains']:

        #         context['domain'] = domain

        #         # Get the bounding box
        #         _domain = config['domains'][domain]

        #         # Subset the domain
        #         lon_slice = slice(_domain['lon_min'], _domain['lon_max'])
        #         lat_slice = slice(_domain['lat_min'], _domain['lat_max'])
        #         _dss = dss_f.sel(lon=lon_slice, lat=lat_slice)

        #         # Assemble global attributes from context
        #         if model['model_lower'][:3] == 'era':
        #             context['description'] = config['templates']['description_era'] % context
        #         else:
        #             context['description'] = config['templates']['description_other'] % context

        #         context['frequency_mapping'] = config['frequency_mapping'][frequency]
        #         context['created'] = datetime.utcnow()
        #         context['uuid'] = uuid4()

        #         global_attrs = dict()

        #         for key, value in config['defaults'].items():
        #             global_attrs[key] = value % context

        #         # Add in meta that we'd like to retain, renaming it in the process
        #         for old_key, new_key in config['retain_metadata'].items():

        #             if old_key in dss.attrs.keys():
        #                 global_attrs[new_key] = dss.attrs[old_key]

        #         # Remove any metadata that is not needed in the output
        #         for rm in config['remove_metadata']:
        #             global_attrs.pop(rm)

        #         # Strip and reapply metadata
        #         _dss.attrs = dict()
        #         _dss.attrs = global_attrs

        #         # Loop through variables and write out data
        #         for variable in _dss.data_vars.keys():

        #             # Update the context
        #             context['variable'] = variable

        #             # Get the full output filepath with string interpolation
        #             output_dir = get_template(config, 'drs_path') % context
        #             output_filename = get_template(config, 'filename') % context
        #             output_filepath = os.path.join(output_dir, output_filename)

        #             # Skip if already there and overwrite is not set, otherwise continue
        #             if os.path.isfile(output_filepath) and args.overwrite == False:
        #                 continue

        #             # Create the output directory
        #             os.makedirs(output_dir, exist_ok=True)

        #             # Assemble the encoding dictionaries (to ensure time units work!)
        #             encoding = config['encoding'].copy()
        #             encoding[variable] = encoding.pop('variables')

        #             # Nested list selection creates a degenerate dataset for per-variable files
        #             _dss[[variable]].to_netcdf(
        #                 output_filepath,
        #                 format='NETCDF4_CLASSIC',
        #                 encoding=encoding
        #             )


    # # Test the difference
    # meta_a = au.extract_metadata(
    #     '/g/data/v14/bjs581/ccam/datastore/raf018/CCAM/WINE/ACCESS1-0/50km/DRS/CORDEX/output/AUS-44i/CSIRO/CSIRO-BOM-ACCESS1-0/historical/r1i1p1/CSIRO-CCAM-1704/v1/mon/tasmax/tasmax_AUS-44i_CSIRO-BOM-ACCESS1-0_historical_r1i1p1_CSIRO-CCAM-1704_v1_mon_200501-200512.nc'
    # )

    # meta_b = au.extract_metadata(
    #     '/g/data/v14/bjs581/ccam/output3/WINE/output/AUS-44i/CSIRO/CSIRO-BOM-ACCESS1-0/r1i1p1/CSIRO-CCAM-1704/v1/month/tasmax/tasmax_AUS-44i_CSIRO-BOM-ACCESS1-0__r1i1p1_CSIRO-CCAM-1704_v1_month_20050101-20141231.nc'
    # )

    # meta_diff = au.diff_metadata(meta_a, meta_b)
    # pprint(meta_diff)
