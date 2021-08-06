"""CCAM DRS post-processing"""
import os
import argparse
import xarray as xr
import glob
import axiom.utilities as au
import axiom_schemas as axs
import numpy as np
import sys
from pprint import pprint
import pandas as pd
from datetime import datetime
from uuid import uuid4


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


if __name__ == '__main__':

    config = au.load_package_data('data/drs.json')

    VALID_YEARS = range(1960, 2090+1)
    VALID_FREQUENCIES = config['valid_frequencies']

    # Valid resolutions and their frequencies
    VALID_RESOLUTIONS = {
        50.0: ['cordex', 'day'],
        12.5 : ['cordex'],
        5.0 : ['1hr', '12min']
    }

    VALID_PROJECTS = config['projects'].keys()
    VALID_MODELS = config['models'].keys()

    # Valid frequencies based on resolutions

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.description = __doc__
    parser.add_argument('input_directory')
    parser.add_argument('output_directory')
    parser.add_argument('-s', '--start_year', required=True, type=int, choices=VALID_YEARS)
    parser.add_argument('-e','--end_year', required=True, type=int, choices=VALID_YEARS)
    parser.add_argument('-f', '--frequency', required=True, type=str, choices=VALID_FREQUENCIES)
    parser.add_argument('-r', '--resolution', required=True, type=int, choices=VALID_RESOLUTIONS.keys())
    parser.add_argument('-p', '--project', required=True, type=str, choices=VALID_PROJECTS)
    parser.add_argument('-m', '--model', required=True, type=str, choices=VALID_MODELS)
    parser.add_argument('-o', '--overwrite', default=False, help='Overwrite existing output', action='store_true')
    parser.add_argument('-v', '--variable', type=str, help='Comma-separated list of variables to process instead of the config.')
    args = parser.parse_args()

    # Check that the frequency requested is valid for the resolution
    assert args.frequency in VALID_RESOLUTIONS[args.resolution]

    # Test that the input directory exists  
    if os.path.exists(args.input_directory) == False:
        sys.exit(1)

    # Load project, model and domain metadata
    project = get_meta(config, 'projects', args.project)
    model = get_meta(config, 'models', args.model)

    # Load domain information based on resolution
    # domains = get_domains(config, args.resolution)

    domains = get_domains(
        resolution=args.resolution,
        frequency=args.frequency,
        variable_fixed=is_fixed_variable(config, args.variable),
        no_frequencies=False
    )

    # Establish the filename base template (this will need to be interpolated)
    filename_base = project['base']

    # Load variables to be processed (2d and 3d)
    variables_2d = project['variables_2d']
    variables_3d = project['variables_3d']

    # Create a dictionary of variables to process keyed to list of levels
    variables = {v2d: list() for v2d in variables_2d}

    # Add in the 3D variables
    for v3d, levels in variables_3d.items():
        variables[v3d] = levels

    # Override with the argument supplied by the user
    # Otherwise all configured variables will run
    if args.variable:
        
        # Allow multiple variables
        user_variables = args.variable.split(',')

        _variables = dict()
        for uv in user_variables:
            _variables[uv] = variables[uv]

        variables = _variables

    # Test that the output directory exists, create if not
    os.makedirs(args.output_directory, exist_ok=True)

    # Loop through the years between start and end in decades
    years = range(args.start_year, args.end_year+1, 10)

    # Assemble the context object (order dependent!)
    context = config['defaults'].copy()
    context.update(project)
    context.update(model)
    context.update(vars(args))

    # Copy version to cordex
    if args.frequency == 'cordex':
        context['rcm_version'] = context['rcm_version_cordex']
        context['rcm_model'] = context['rcm_model_cordex']

    for year in years:

        # Work out the end year based on some hard-coded rules
        start_year = year
        end_year = start_year + 9

        if start_year == 2006:
            end_year = 2009

        # Non-ERA 
        elif start_year == 2000 and model['model_lower'][:3] != 'era':
            end_year = 2005
        
        # ERA
        elif start_year == 2000 and model['model_lower'][:3] != 'era':
            end_year = 2014
        
        context['start_date'] = f'{start_year}0101'
        context['end_date'] = f'{end_year}1231'

        input_filename = filename_base % context
        input_filename = f'{input_filename}.{year}??.nc'
        input_filepath_search = os.path.join(
            args.input_directory,
            input_filename
        )

        input_filepaths = sorted(glob.glob(input_filepath_search))
        assert len(input_filepaths) > 0

        # Load the input files
        dss = xr.open_mfdataset(
            input_filepaths,
            chunks=dict(time=1)
        )

        # Standardise the units
        dss = standardise_units(dss)

        # Subset the variables requested
        dss = dss[list(variables.keys())]

        # Select the levels for any 3D variables in the dataset
        for variable, levels in variables.items():
            for level in levels:
                dss[f'{variable}{level}'] = dss.sel(lev=level, drop=True)

        # Remove first timestep 00hr of each month of CORDEX file
        if args.frequency == 'cordex':
            dss = dss.isel(time=slice(1, None))

        # Work out the correct schema to apply
        # TODO: need to add the fixed variables?
        if args.frequency == 'month':
            schema_filename = 'cordex-month.json'
        else:
            schema_filename = 'cordex-day.json'

        # Apply it
        schema = axs.load_schema(schema_filename)
        dss = au.apply_schema(dss, schema)

        # Loop through each frequency
        for frequency in domains['frequencies']:
            
            # Perform the resampling to get the frequency required
            dss_f = dss.resample(time=frequency).mean()

            # Loop through each domain
            for domain in domains['domains']:

                context['domain'] = domain

                # Get the bounding box
                _domain = config['domains'][domain]

                # Subset the domain
                lon_slice = slice(_domain['lon_min'], _domain['lon_max'])
                lat_slice = slice(_domain['lat_min'], _domain['lat_max'])
                _dss = dss_f.sel(lon=lon_slice, lat=lat_slice)

                # Assemble global attributes from context

                if model['model_lower'][:3] == 'era':
                    context['description'] = config['templates']['description_era'] % context
                else:
                    context['description'] = config['templates']['description_other'] % context
                
                context['frequency_mapping'] = config['frequency_mapping'][frequency]
                context['created'] = datetime.utcnow()
                context['uuid'] = uuid4()
                
                global_attrs = dict()

                for key, value in config['defaults'].items():
                    global_attrs[key] = value % context
                
                # Add in meta that we'd like to retain, renaming it in the process
                for old_key, new_key in config['retain_metadata'].items():
                    
                    if old_key in dss.attrs.keys():
                        global_attrs[new_key] = dss.attrs[old_key]

                # Remove any metadata that is not needed in the output
                for rm in config['remove_metadata']:
                    global_attrs.pop(rm)

                # Strip and reapply metadata
                _dss.attrs = dict()
                _dss.attrs = global_attrs

                # Loop through variables and write out data
                for variable in _dss.data_vars.keys():

                    # Update the context
                    context['variable'] = variable

                    # Get the full output filepath with string interpolation
                    output_dir = get_template(config, 'drs_path') % context
                    output_filename = get_template(config, 'filename') % context
                    output_filepath = os.path.join(output_dir, output_filename)

                    # Skip if already there and overwrite is not set, otherwise continue
                    if os.path.isfile(output_filepath) and args.overwrite == False:
                        continue

                    # Create the output directory
                    os.makedirs(output_dir, exist_ok=True)

                    # Assemble the encoding dictionaries (to ensure time units work!)
                    encoding = config['encoding'].copy()
                    encoding[variable] = encoding.pop('variables')

                    # Nested list selection creates a degenerate dataset for per-variable files
                    _dss[[variable]].to_netcdf(
                        output_filepath,
                        format='NETCDF4_CLASSIC',
                        encoding=encoding
                    )
        

    # Test the difference
    meta_a = au.extract_metadata(
        '/Users/sch576/work/axiom/ccam_data/tasmax_AUS-44i_NCC-NorESM1-M_rcp45_r1i1p1_CSIRO-CCAM-1704_v1_day_20060101-20061231.nc'
    )

    meta_b = au.extract_metadata(
        '/Users/sch576/work/axiom/ccam_data/output/DELWP/output/AUS-44i/CSIRO/NCC-NorESM1-M/rcp45/r1i1p1/CSIRO-CCAM-1704/v1/day/tasmax/tasmax_AUS-44i_NCC-NorESM1-M_rcp45_r1i1p1_CSIRO-CCAM-1704_v1_day_20060101-20091231.nc'
    )

    meta_diff = au.diff_metadata(meta_a, meta_b)
    pprint(meta_diff)