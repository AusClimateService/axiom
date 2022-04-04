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
from axiom.config import load_config
from axiom import __version__ as axiom_version


def consume(json_filepath):
    """Consume a json payload (for message passing)

    Args:
        json_filepath (str): Path to the JSON file.
    """
    logger = au.get_logger(__name__)

    # Check if the file has already been consumed
    consumed_filepath = json_filepath.replace('.json', '.consumed')
    if os.path.isfile(consumed_filepath):
        logger.info(f'{json_filepath} has already been consumed and needs to be cleaned up by another process. Terminating.')
        sys.exit()
    

    # Check if the file is locked
    if au.is_locked(json_filepath):
        logger.info(f'{json_filepath} is locked, possibly by another process. Terminating.')
        sys.exit()
        
    # Lock the file
    au.lock(json_filepath)

    # Convert to dict
    payload = json.loads(open(json_filepath, 'r').read())

    # Process
    process_multi(**payload)

    # Mark consumed by touching another file.
    au.touch(consumed_filepath)

    # Unlock
    au.unlock(json_filepath)


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
    project,
    model,
    domain,
    start_year, end_year,
    cordex,
    output_frequency,
    level=None,
    input_resolution=None, 
    overwrite=True,
    preprocessor=None,
    postprocessor=None,
    **kwargs
    ):
    """Method to process a single variable/domain/resolution combination.

    Args:
        input_files (str or list): Globbable string or list of filepaths.
        output_directory (str) : Path from which to build DRS structure.
        variable (str): Variable to process.
        level (numeric or list) : Vertical levels to process.
        project (str): Project metadata to apply (loaded from user config).
        model (str): Model metadata to apply (loaded from user config).
        start_year (int): Start year.
        end_year (int): End year.
        cordex (bool): Activate cordex processing.
        output_frequency (str): Output frequency to process.
        input_resolution (float, optional): Input resolution in km. Leave black to auto-detect from filepaths.
        overwrite (bool): Overwrite the data at the destination. Defaults to True.
        preprocessor (str): Data preprocessor to activate on input data. Defaults to None.
        postprocesser (str): Data postprocess to activate before writing data. Defaults to None.
        **kwargs: Additional keyword arguments used in metadata interpolation.
    """

    # Start the clock
    timer = au.Timer()
    timer.start()

    # Capture what was passed into this method for interpolation context later.
    local_args = locals()

    # Load the logger and configuration
    logger = au.get_logger(__name__)
    config = load_config('drs')

    # Get a list of the potential filepaths
    input_files = au.auto_glob(input_files)
    num_files = len(input_files)
    logger.debug(f'{num_files} to consider before filtering.')

    # Filter by those that actually have the variable in the filename.
    if config.filename_filtering['variable']:
        input_files = [f for f in input_files if f'{variable}_' in os.path.basename(f)]
        num_files = len(input_files)
        logger.debug(f'{num_files} to consider after filename variable filtering.')

    # Filter by those that actually have the year in the filename.
    if config.filename_filtering['year']:
        input_files = [f for f in input_files if f'{start_year}' in os.path.basename(f)]
        num_files = len(input_files)
        logger.debug(f'{num_files} to consider after filename year filtering.')
    
    # Is there anything left to process?
    if len(input_files) == 0:
        logger.info('No files to process.')
        return

    # Detect the input resolution if it it not supplied
    if input_resolution is None:
        logger.debug('No input resolution supplied, auto-detecting')
        input_resolution = adu.detect_resolution(input_files)
        logger.debug(f'Input resolution detected as {input_resolution} km')
    

    # Load project config
    logger.info(f'Loading project config ({project})')
    project = load_config('projects')[project]
    
    # Load model config
    logger.info(f'Loading model config ({model})')
    model = load_config('models')[model]

    # Ensure the output directories exist
    os.makedirs(output_directory, exist_ok=True)

    logger.debug('Loading files into distributed memory, this may take some time.')

    # Load a preprocessor, if one exists.
    preprocessor = adu.load_processor(preprocessor, 'pre')
    preprocess = lambda ds: preprocessor(ds, variable)
    ds = xr.open_mfdataset(input_files, chunks=dict(time=100), preprocess=preprocess)

    # Assemble the context object (order dependent!)
    logger.debug('Assembling interpolation context.')
    # context = config.section2dict('metadata.defaults')
    context = config.metadata_defaults.copy()

    # Add metadata from the input data
    context.update(ds.attrs)

    # Add user-supplied metadata
    context.update(kwargs)

    # Add project and model metadata
    context.update(project)
    context.update(model)

    # Add additional args
    context.update(local_args)
    context['res_km'] = input_resolution

    # Select the variable from the dataset
    if level:
        
        # Select each of the levels requested into a new variable.
        for _level in au.pluralise(level):
            ds[f'{variable}{_level}'] = ds[variable].sel(lev=_level, drop=True)
        
        # Drop the original variable
        ds = ds.drop(variable)

    # Sort the dimensions (fixes domain subsetting)
    logger.debug('Sorting data')
    ds = ds.sortby(['time', 'lat', 'lon'])

    # Work out which cordex schema to use based on output frequency
    # if cordex:
    #     logger.debug('Applying metadata schema')
    #     if 'M' in output_frequency:
    #         schema = axs.load_schema('cordex-month.json')
    #     else:
    #         schema = axs.load_schema('cordex-day.json')

    #     ds = au.apply_schema(ds, schema)

    logger.debug('Applying metadata schema')
    schema = axs.load_schema(config['schema'])
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
        
        # Historical cutoff is defined in $HOME/.axiom/drs.ini
        context['experiment'] = 'historical' if year < config.historical_cutoff else context['rcp']

        # Resample the data to the desired frequency
        logger.debug(f'Resampling to {output_frequency} mean.')
        _ds = _ds.resample(time=output_frequency).mean()

        # TODO: Add cell methods?
        
        # Monthly data should have the days truncated
        context['start_date'] = f'{year}0101' if output_frequency[-1] != 'M' else f'{year}01'
        context['end_date'] = f'{year}1231' if output_frequency[-1] != 'M' else f'{year}12'

        # Map the frequency to something DRS-compliant
        context['frequency_mapping'] = config['frequency_mapping'][output_frequency]

        # Tracking info
        context['created'] = datetime.utcnow()
        context['uuid'] = uuid4()

        # Interpolate context
        logger.info('Interpolating context.')
        context = adu.interpolate_context(context)

        # Assemble the global meta, add axiom details
        logger.debug('Assembling global metadata.')
        global_attrs = dict(
            axiom_version=axiom_version,
            axiom_schemas_version=axs.__version__,
            axiom_schema=config.schema
        )

        for key, value in config.metadata_defaults.items():
            global_attrs[key] = str(value) % context

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
        encoding = dict()

        for coord in list(_ds.coords.keys()):
            encoding[coord] = config.encoding[coord]

        # Apply a blanket variable encoding.
        encoding[variable] = config.encoding['variables']

        # Postprocess data if required
        postprocess = adu.load_processor(postprocessor, 'post')
        _ds = postprocess(_ds)

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
    
    elapsed_time = timer.stop()
    logger.info(f'DRS processing task took {elapsed_time} seconds.')


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


def process_multi(variables, domain, project, **kwargs):

    logger = au.get_logger(__name__)

    # Load the project metadata
    project_config = load_config('projects')[project]
    config = load_config('drs')

    # Load all variables if nothing was supplied
    if not variables:
        schema_file = config['schema']
        logger.info(f'No variables supplied, loading from schema as defined in configuration ({schema_file}).')
        schema = axs.load_schema(schema_file)
        variables = list(schema['variables'].keys())

    num_variables = len(variables)
    logger.info(f'{num_variables} variable(s) to process.')
    # sys.exit()

    # Start the cluster if requested
    if config.dask['enable']:
        logger.info('Starting dask client.')
        cluster = LocalCluster(**config.dask['cluster'])
        client = Client(cluster)
        logger.info(client)

    # Yes this is a nested loop, but a single variable/domain/output_freq combination could still be 10K+ files, which WILL be processed in parallel.
    # for variable, levels in variables.items():
    for variable in variables:

        try:
            # for level in levels:
            logger.info(f'Processing {variable}')
            instance_kwargs = kwargs.copy()
            instance_kwargs['variable'] = variable
            # instance_kwargs['level'] = level
            instance_kwargs['domain'] = domain
            instance_kwargs['project'] = project

            process(**instance_kwargs)

        except Exception as ex:

            logger.error(f'Variable {variable} failed.')
