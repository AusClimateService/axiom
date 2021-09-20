"""Main entrypoint for DRS processing."""
import os
import argparse
from datetime import datetime
from uuid import uuid4
import xarray as xr
import axiom.utilities as au
import axiom.drs.utilities as adu
import axiom_schemas as axs


def get_parser(config=None, parent=None):
    """Parse arguments for command line utiltities.

    Returns:
        argparse.Namespace : Arguments object.
    """

    if config is None:
        config = au.load_package_data('data/drs.json')

    # Pull some config
    VALID_PROJECTS = config['projects'].keys()
    VALID_MODELS = config['models'].keys()
    VALID_DOMAINS = config['domains'].keys()

    # Build a parser, or add one to the top-level CLI
    if parent is None:
        parser = argparse.ArgumentParser()
    else:
        parser = parent.add_parser('drs')

    parser.description = "DRS utility"

    # Paths
    parser.add_argument('input_files', type=str, help='Input filepaths', nargs="+")
    parser.add_argument('output_directory', type=str, help='Output base directory (DRS structure built from here)')
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

    return parser


# def main(config=None, args=None):
def main(input_files, output_directory, start_year, end_year, output_frequency, project, model, variable, domains, cordex=False, input_resolution=None):
    """Process the input files into DRS format.

    Args:
        input_files (list): List of filepaths.
        output_directory (str): Output directory (DRS path built from here)
        start_year (int): Starting year.
        end_year (int): Ending year.
        output_frequency (str): Output frequency.
        project (str): Project code.
        model (str): Model key
        variable (str): Variable.
        domains (list) : List of domains to process.
        cordex (bool) : Process for cordex.
        input_resolution (float, optional): Input resolution in km. Defaults to None, detected from filepaths.
    """

    local_args = locals()

    logger = au.get_logger(__name__)

    # Load the DRS configuration from Axiom.
    config = au.load_package_data('data/drs.json')

    # Detect the input resolution if it it not supplied
    if input_resolution is None:
        logger.debug('No input resolution supplied, auto-detecting')
        input_resolution = adu.detect_resolution(input_files)
        logger.debug(f'Input resolution detected as {input_resolution} km')

    # Test that the input files exist
    logger.debug('Ensuring all input files actually exist.')
    assert adu.input_files_exist(input_files)

    # Load project, model and domain metadata
    logger.debug(f'Loading project ({project}) and model ({model}) metadata.')
    project = adu.get_meta(config, 'projects', project)
    model = adu.get_meta(config, 'models', model)

    # Establish the filename base template (this will need to be interpolated)
    filename_base = project['base']

    if variable:

        logger.debug(f'User has supplied variables ({variable}).')
        variables = {v: list() for v in variable}

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
    logger.debug(f'Creating {output_directory}')
    os.makedirs(output_directory, exist_ok=True)

    # Loop through the years between start and end in decades
    logger.debug('Constructing a list of years to process')
    years = range(start_year, end_year+1, 10)

    # Assemble the context object (order dependent!)
    logger.debug('Assembling interpolation context.')
    context = config['defaults'].copy()
    context.update(project)
    context.update(model)

    # Add the local arguments tot he context for compatibility
    context.update(local_args)

    # Add rcm metadata
    if cordex:
        logger.debug('User has requested CORDEX processing, adding rcm metadata')
        context['rcm_version'] = context['rcm_version_cordex']
        context['rcm_model'] = context['rcm_model_cordex']

    # Open all the files
    logger.debug('Loading files into distributed memory, this may take some time.')

    # Remove the first timestep from each cordex monthly file
    if cordex:
        logger.debug('Preprocessing cordex inputs.')
        dss = xr.open_mfdataset(input_files, chunks=dict(time=1), preprocess=adu.preprocess_cordex)
    else:
        dss = xr.open_mfdataset(input_files, chunks=dict(time=1))

    logger.debug('Standardising units')
    dss = adu.standardise_units(dss)

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

    # Work out which schema to use based on output frequency
    logger.debug('Applying metadata schema')
    if 'M' in output_frequency:
        schema = axs.load_schema('cordex-month.json')
    else:
        schema = axs.load_schema('cordex-day.json')

    dss = au.apply_schema(dss, schema)

    # Process each year
    logger.debug('Starting processing')

    # Validate the domains
    valid_domains = dict()
    for domain in domains:

        # Predefined domain
        if domain in config['domains'].keys():
            valid_domains[domain] = config['domains'][domain]
        
        # Arbitrary domain
        else:
            domain = adu.parse_domain_directive(domain)
            valid_domains[domain['name']] = domain

    # Loop through each year
    for year in years:

        start_year = year

        # TODO: Need a cleaner way to do this
        if start_year < 2006:
            context['experiment'] = 'historical'
        else:
            context['experiment'] = context['rcp']
        #
        if context['gcm_model'] in ['ERAINT', 'ERA5']:
            context['experiment'] = 'evaluation'
            context['description'] = adu.get_template(config, 'description_era') % context
        else:
            description_template = 'description_other'
            context['description'] = adu.get_template(config, 'description_other') % context

        start_date = f'{year}0101'
        end_date = f'{year}1231'

        logger.debug(f'start_date = {start_date}')
        logger.debug(f'end_date = {end_date}')
        logger.debug(f'Processing {year}')

        # Loop through each output frequency
        for _output_frequency in output_frequency:

            # Update the dates based on the frequency
            if _output_frequency == '1M':
                context['start_date'] = start_date[:-2] # remove the days
                context['end_date'] = end_date[:-2]
            else:
                context['start_date'] = start_date
                context['end_date'] = end_date

            logger.debug(f'Processing {_output_frequency} frequency')

            logger.debug(f'Resampling to {_output_frequency}')
            dss_f = dss.resample(time=_output_frequency).mean()

            for domain_name, domain in valid_domains.items():

                logger.debug(f'Processing {domain_name}')
                context['domain'] = domain_name

                # Allow arbitrary domains
                _domain = config['domains'][domain]

                logger.debug('Subsetting domain')
                logger.debug(domain)

                # Fix to cross the meridian
                if domain['lon_max'] < domain['lon_min']:
                    logger.debug('Domain crosses the meridian')
                    lon_constraint = (dss_f.lon <= domain['lon_min']) | (dss_f.lon >= domain['lon_max'])
                else:
                    lon_constraint = (dss_f.lon >= domain['lon_min']) & (dss_f.lon <= domain['lon_max'])

                # Add latitudes
                lat_constraint = (dss_f.lat >= domain['lat_min']) & (dss_f.lat <= domain['lat_max'])
                constraint = lon_constraint & lat_constraint

                # Subset the domain
                dss_d = dss_f.where(constraint, drop=True)

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
                        logger.debug(f'Retaining {old_key}, renamed to {new_key}')
                        global_attrs[new_key] = dss.attrs[old_key]

                # Remove any metadata that is not needed in the output
                logger.debug('Removing requested metadata from inputs')
                for rm in config['remove_metadata']:
                    logger.debug(f'Removing {rm}')
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
                    output_dir = adu.get_template(config, 'drs_path') % context
                    output_filename = adu.get_template(
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
                    dss_d = adu.postprocess_cordex(dss_d)

                    # Nested list selection creates a degenerate dataset for per-variable files
                    logger.debug(f'Writing {output_filepath}')
                    dss_d[[variable]].to_netcdf(
                        output_filepath,
                        format='NETCDF4_CLASSIC',
                        encoding=encoding,
                        unlimited_dims=['time']
                    )


if __name__ == '__main__':
    config = au.load_package_data('data/drs.json')
    main(config=config)