"""Main entrypoint for DRS processing."""
import os
import argparse
from datetime import datetime
from uuid import uuid4
import xarray as xr
import axiom.utilities as au
import axiom.drs.utilities as adu
from axiom.drs.domain import Domain
import axiom_schemas as axs
import json
import sys
from distributed import Client, LocalCluster


def consume(json_filepath):
    """Consume a json payload (for message passing)

    Args:
        json_filepath (str): Path to the JSON file.
    """
    # Convert to dict
    payload = json.loads(open(json_filepath, 'r').read())

    # print(payload)
    # sys.exit()

    # Pass to main
    process_multi(**payload)

    # TODO: Mark as consumed at the end
    # TODO: Cleanup payloads at the end
    # TODO: Cleanup partial completions before starting (check timestamp, day old unlock)

def main(input_files, output_directory, start_year, end_year, output_frequency, project, model, variables, domains, cordex=False, input_resolution=None, overwrite=False, **kwargs):
    """Process the input files into DRS format.

    Args:
        input_files (list): List of filepaths.
        output_directory (str): Output directory (DRS path built from here)
        start_year (int): Starting year.
        end_year (int): Ending year.
        output_frequency (str): Output frequency.
        project (str): Project code.
        model (str): Model key
        variables (str): Variable.
        domains (list) : List of domains to process.
        cordex (bool) : Process for cordex.
        input_resolution (float, optional): Input resolution in km. Defaults to None, detected from filepaths.
        overwrite (bool, optional): Overwrite outputs. Defaults to False.
        **kwargs : Reserved for rapid development
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

    # Automatic globbing of input files
    input_files = au.auto_glob(input_files)
    num_input_files = len(input_files)

    # Load project, model and domain metadata
    logger.debug(f'Loading project ({project}) and model ({model}) metadata.')
    project = adu.get_meta(config, 'projects', project)
    model = adu.get_meta(config, 'models', model)

    # Establish the filename base template (this will need to be interpolated)
    filename_base = project['base']

    if variables:

        logger.debug(f'User has supplied variables ({variables}).')
        variables = {v: list() for v in variables}

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

    # Standardise the units.
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
        # TODO: Add multiple frequencies, this will required updating the CLI
        for _output_frequency in [output_frequency]:

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
                _domain = config['domains'][domain_name]

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
                    if os.path.isfile(output_filepath) and overwrite == False:
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
                    dss_d[[variable]].load().to_netcdf(
                        output_filepath,
                        format='NETCDF4_CLASSIC',
                        encoding=encoding,
                        unlimited_dims=['time']
                    )

def process(
    input_files,
    output_directory,
    variable,
    level,
    project,
    model,
    domain,
    start_year, end_year,
    cordex,
    output_frequency,
    input_resolution=None, 
    overwrite=True,
    **kwargs
    ):
    """Method to process a single variable/domain/resolution combination.

    Args:
        input_files (str or list): Globbable string or list of filepaths.
        output_directory (str) : Path from which to build DRS structure.
        variable (str): Variable to process.
        project (str): Project metadata to apply.
        model (str): Model metadata to apply.
        input_resolution (float, optional): Input resolution in km. Leave black to auto-detect from filenames.
    """

    # Capture what was passed into this method for interpolation context later.
    local_args = locals()

    # Load the logger and configuration
    logger = au.get_logger(__name__)
    config = au.load_package_data('data/drs.json')

    if config['use_dask'] == True:
        logger.info('Starting client')
        client = Client()
        print(client)

    # Get a list of the potential filepaths
    input_files = au.auto_glob(input_files)
    num_files = len(input_files)
    logger.debug(f'{num_files} to consider before filtering.')

    # Filter by those that actually have the variable in the filename.
    if config['filename_filtering']['variable'] == True:
        input_files = [f for f in input_files if f'{variable}_' in os.path.basename(f)]
        num_files = len(input_files)
        logger.debug(f'{num_files} to consider after filename variable filtering.')

    # Filter by those that actually have the year in the filename.
    if config['filename_filtering']['year'] == True:
        input_files = [f for f in input_files if f'{start_year}' in os.path.basename(f)]
        num_files = len(input_files)
        logger.debug(f'{num_files} to consider after filename year filtering.')
    
    # Is there anything left to process?
    if len(input_files) == 0:
        logger.info('No files to process.')
        return

    # Detect the input resolution if it it not supplied
    # TODO: Could derive from input data.
    if input_resolution is None:
        logger.debug('No input resolution supplied, auto-detecting')
        input_resolution = adu.detect_resolution(input_files)
        logger.debug(f'Input resolution detected as {input_resolution} km')

    # Load project and model metadata
    logger.debug(f'Loading project ({project}) and model ({model}) metadata.')
    project = adu.get_meta(config, 'projects', project)
    model_str = model
    model = adu.get_meta(config, 'models', model)

    # Ensure the output directories exist
    os.makedirs(output_directory, exist_ok=True)

    # Assemble the context object (order dependent!)
    logger.debug('Assembling interpolation context.')
    context = config['defaults'].copy()
    context.update(project)
    context.update(model)
    context.update(local_args)

    logger.debug('Loading files into distributed memory, this may take some time.')

    # CORDEX processing triggers additional metadata and preprocessing
    if cordex:
        logger.debug('User has requested CORDEX processing, adding rcm metadata')
        context['rcm_version'] = context['rcm_version_cordex']
        context['rcm_model'] = context['rcm_model_cordex']

    # Special case for CCAM processing.
    # TODO: Check
    if 'ccam' in model_str.lower():
        ds = xr.open_mfdataset(input_files, chunks=dict(time=1), preprocess=adu.preprocess_ccam)
    else:
        ds = xr.open_mfdataset(input_files, chunks=dict(time=1))

    # TODO: Convert/standardise the units automatically?
    # Units should be correct before hitting axiom

    # Select the variable from the dataset
    if level is None:
        da = ds[variable]
    
    # If we are processing a level, then we extract and rename
    # Note: This can be changed to 3D data later.
    else:
        da = ds[variable].sel(lev=level, drop=True)
        da.name = f'{variable}{level}'

    # Convert back into a dataset for schema application
    ds = da.to_dataset()

    # Sort the dimensions (fixes domain subsetting)
    logger.debug('Sorting data')
    ds = ds.sortby(['time', 'lat', 'lon'])

    # Work out which cordex schema to use based on output frequency
    if cordex:
        logger.debug('Applying metadata schema')
        if 'M' in output_frequency:
            schema = axs.load_schema('cordex-month.json')
        else:
            schema = axs.load_schema('cordex-day.json')

        ds = au.apply_schema(ds, schema)

    logger.info(f'Parsing domain {domain}')
    if isinstance(domain, str):

        # Registered domain
        if adu.is_registered_domain(domain):
            domain = adu.get_domain(domain)
        
        # Attempt to parse
        else:
            domain = Domain.from_directive(domain)

    # We will only otherwise accept a domain object.
    elif isinstance(domain, Domain) == False:
        raise Exception(f'Unable to parse domain {domain}.')

    logger.debug('Domain: ' + domain.to_directive())

    # Subset the geographical domain
    logger.debug('Subsetting geographical domain.')
    ds = domain.subset_xarray(ds, drop=True)

    # TODO: Need to find a less manual way o do this.
    for year in generate_years_list(start_year, end_year):

        logger.info(f'Processing {year}')

        # Subset the data into just this year
        _ds = ds.where(ds['time.year'] == year, drop=True)
        
        # Pre-2006 data is historical (TODO: pre 2014 historical for CMIP6)
        # TODO: Make this configurable in the configs or pull from the model?
        context['experiment'] = 'historical' if year < 2006 else context['rcp']

        # Special cases for ERA data
        if context['gcm_model'] in ['ERAINT', 'ERA5']:
            context['experiment'] = 'evaluation'
            context['description'] = adu.get_template(config, 'description_era') % context
        else:
            context['description'] = adu.get_template(config, 'description_other') % context

        # Resample the data to the desired frequency
        logger.debug(f'Resampling to {output_frequency} mean.')
        _ds = _ds.resample(time=output_frequency).mean()
        
        # Monthly data should have the days truncated
        context['start_date'] = f'{year}0101' if output_frequency[-1] != 'M' else f'{year}01'
        context['end_date'] = f'{year}1231' if output_frequency[-1] != 'M' else f'{year}12'

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
            if old_key in _ds.attrs.keys():
                logger.debug(f'Retaining {old_key}, renamed to {new_key}')
                global_attrs[new_key] = _ds.attrs[old_key]

        # Remove any metadata that is not needed in the output
        logger.debug('Removing requested metadata from inputs')
        for rm in config['remove_metadata']:
            logger.debug(f'Removing {rm}')
            global_attrs.pop(rm)

        # Strip and reapply metadata
        logger.debug('Applying metadata')
        _ds.attrs = global_attrs

        # Add in the variable to the context
        context['variable'] = variable

        # Reapply the schema
        logger.info('Reapplying schema')
        _ds = au.apply_schema(_ds, schema)

        # Get the full output filepath with string interpolation
        logger.debug('Working out output paths')
        drs_path = adu.get_template(config, 'drs_path') % context
        output_filename = adu.get_template(config, 'filename') % context
        output_filepath = os.path.join(output_directory, drs_path, output_filename)
        logger.debug(f'output_filepath = {output_filepath}')

        # Skip if already there and overwrite is not set, otherwise continue
        if os.path.isfile(output_filepath) and overwrite == False:
            logger.debug(f'{output_filepath} exists and overwrite is set to False, skipping.')
            continue

        # Create the output directory
        output_dir = os.path.dirname(output_filepath)
        logger.debug(f'Creating {output_dir}')
        os.makedirs(output_dir, exist_ok=True)

        # Assemble the encoding dictionaries (to ensure time units work!)
        logger.debug('Applying encoding')
        encoding = config['encoding'].copy()
        encoding[variable] = encoding.pop('variables')

        # Center the months etc.
        # TODO: Postprocess CCAM?
        _ds = adu.postprocess_cordex(_ds)

        # Nested list selection creates a degenerate dataset for per-variable files
        # TODO: Check to make sure that the file doesnt already exist
        # Do this at the top.
        logger.debug(f'Writing {output_filepath}')
        _ds.to_netcdf(
            output_filepath,
            format='NETCDF4_CLASSIC',
            encoding=encoding,
            unlimited_dims=['time']
        )

        print(_ds)
        sys.exit()

    # Assemble the remaining metadata


    print(ds)
    sys.exit()


def generate_years_list(start_year, end_year):
    """Generate a list of years (decades) to process.

    Args:
        start_year (int): Start year.
        end_year (int): End year.

    Returns:
        iterator : Years to process.
    """
    return range(start_year, end_year+1, 10)


def load_variable_config(project_config):
    
    # Extract the different rank variables
    v2ds = project_config['variables_2d']
    v3ds = project_config['variables_3d']

    # Create a dictionary of variables to process keyed to an empty list of levels for 2D
    variables = {v2d: [None] for v2d in v2ds}
    
    # Add in the 3D variables, with levels this time
    for v3d, levels in v3ds.items():
        variables[v3d] = levels

    return variables


def process_multi(variables, domains, project, **kwargs):

    logger = au.get_logger(__name__)

    # Load the DRS configuration from Axiom.
    config = au.load_package_data('data/drs.json')

    # Load the project metadata
    project_config = adu.get_meta(config, 'projects', project)

    # Load all variables if nothing was supplied
    if variables is None or variables[0] == '':
        logger.info('No variables supplied, loading from config.')
        variables = load_variable_config(project_config)
    
    # Yes this is a nested loop, but a single variable/domain/output_freq combination could still be 10K+ files, which WILL be processed in parallel.
    for variable, levels in variables.items():
        for level in levels:
            for domain in domains:
                # Create a set of arguments for a single processing chain
                instance_kwargs = kwargs.copy()
                instance_kwargs['variable'] = variable
                instance_kwargs['level'] = level
                instance_kwargs['domain'] = domain
                instance_kwargs['project'] = project

                process(**instance_kwargs)


if __name__ == '__main__':
    config = au.load_package_data('data/drs.json')
    main(config=config)
