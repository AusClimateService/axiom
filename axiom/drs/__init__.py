"""Main entrypoint for DRS processing."""
from genericpath import isfile
import os
import argparse
from datetime import datetime
from uuid import uuid4
import xarray as xr
import axiom.utilities as au
import axiom.drs.utilities as adu
from axiom.drs.domain import Domain
import axiom.schemas as axs
import json
import sys
from distributed import Client, LocalCluster
from axiom.config import load_config
from axiom import __version__ as axiom_version
from axiom.exceptions import NoFilesToProcessException, DRSContextInterpolationException
import shutil
from dask.distributed import progress, wait
import numpy as np
from axiom.supervisor import Supervisor


def consume(json_filepath):
    """Consume a json payload (for message passing)

    Args:
        json_filepath (str): Path to the JSON file.
    """
    logger = au.get_logger(__name__)

    # Check if the file has already been consumed
    consumed_filepath = json_filepath.replace('.json', '.consumed')
    if os.path.isfile(consumed_filepath):
        logger.info(
            f'{json_filepath} has already been consumed and needs to be cleaned up by another process. Terminating.')
        sys.exit()

    # Check if the file is locked
    if au.is_locked(json_filepath):
        logger.info(
            f'{json_filepath} is locked, possibly by another process. Terminating.')
        sys.exit()

    # Lock the file
    au.lock(json_filepath)

    # Convert to dict
    payload = json.loads(open(json_filepath, 'r').read())

    # Allow rerun of failed variables (do this after all other variables have been processed!)
    config = load_config('drs')
    failures_path = f'{json_filepath}_001.failed'
    if config.rerun_failures and os.path.exists(failures_path):
        failed_variables = open(failures_path, 'r').read().splitlines()
        payload['variables'] = failed_variables

    # Process
    process_multi(**payload)

    # Mark consumed by touching another file.
    au.touch(consumed_filepath)

    # Unlock
    au.unlock(json_filepath)

    # Explicit exit (#125)
    sys.exit(0)


def process(
    input_files,
    output_directory,
    variable,
    project,
    model,
    domain,
    start_year, end_year,
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
        project (str): Project metadata to apply (loaded from user config).
        model (str): Model metadata to apply (loaded from user config).
        start_year (int): Start year.
        end_year (int): End year.
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

    # Dump the job id if available
    if 'PBS_JOBID' in os.environ.keys():
        jobid = os.getenv('PBS_JOBID')
        logger.info(f'My PBS_JOBID is {jobid}')

    logger.info('Searching for files matching the following path:')
    logger.info(input_files)

    # Get a list of the potential filepaths
    input_files = au.auto_glob(input_files)
    num_files = len(input_files)
    logger.debug(f'{num_files} to consider before filtering.')

    # Filter by those that actually have the variable in the filename.
    if config.filename_filtering['variable']:

        input_files = adu.filter_by_variable_name(input_files, variable)
        num_files = len(input_files)
        logger.debug(
            f'{num_files} to consider after filename variable filtering.')

    # Filter by those that actually have the year in the filename (plus or minus an offset).
    if config.filename_filtering['year']:
        
        input_files = filter_years(
            input_files, start_year, offset=config.filename_filtering['year_offset'])
        num_files = len(input_files)
        logger.debug(f'{num_files} to consider after filename year filtering.')

    # Is there anything left to process?
    if len(input_files) == 0:
        raise NoFilesToProcessException()

    # Detect the input resolution if it it not supplied
    if input_resolution is None:
        logger.debug('No input resolution supplied, auto-detecting')
        input_resolution = adu.detect_resolution(input_files)
        logger.debug(f'Input resolution detected as {input_resolution} km')

    # Load project config
    logger.info(f'Loading project config ({project})')
    project_key = project
    project = load_config('projects')[project_key]

    # Ensure it actually exists
    assert isinstance(project, dict), f'Project {project_key} not found, does it exist in projects.json?'

    # Load model config
    logger.info(f'Loading model config ({model})')
    model_key = model
    model = load_config('models')[model_key]

    # Ensure it actually exists
    assert isinstance(model, dict), f'Model {model_key} not found, does it exist in models.json?'

    logger.debug(
        'Loading files into distributed memory, this may take some time.')

    # TODO: Remove!!!! This is just to make CCAM work in the short term
    if preprocessor is None and 'ccam' in input_files[0] and config.auto_detect_ccam == True:
        logger.warn('CCAM preprocessor override used')
        preprocessor = 'ccam'
        postprocessor = 'ccam'

    # Load a preprocessor, if one exists.
    preprocessor = adu.load_preprocessor(preprocessor)
    def preprocess(ds, *args, **kwargs): return preprocessor(ds, **local_args)

    # Load the open_dataset configuration
    open_dataset_kwargs = config['xarray']['open_dataset']

    # Account for fixed variables, if defined
    if 'variables_fixed' in project.keys() and variable in project['variables_fixed']:

        # Load just the first file
        ds = xr.open_dataset(input_files[0], **open_dataset_kwargs)
        ds = preprocess(ds, variable=variable)

    else:
        
        ds = xr.open_mfdataset(
            input_files,
            preprocess=preprocess,
            **open_dataset_kwargs
        )

    # Subset temporally
    if not adu.is_time_invariant(ds):
        logger.info(f'Subsetting times to {start_year}')
        # ixs = np.where(ds['time.year'] == start_year)
        time_slice = slice(f'{start_year}-01-01', f'{start_year}-12-31')
        # ds = ds.isel(time=slice(ixs[0][0], ixs[0][-1] + 1))
        ds = ds.sel(time=time_slice, drop=True)

    # Skip over the file if subdaily resampling is disabled, this will stop 
    native_frequency = adu.detect_input_frequency(ds)

    # Ensure blank output frequency is indeed fixed and only one can be written
    if adu.is_time_invariant(ds):
        output_frequency = 'fx'
        overwrite = False

    logger.info(f'native_frequency = {native_frequency}, output_frequency = {output_frequency}')
    if config.allow_subdaily_resampling == False and native_frequency != output_frequency and 'H' in output_frequency:
        logger.info(f'Subdaily resampling has been disabled and input/output frequencies do not match, skipping {variable}.')
        return

    # Persist now, get it on the cluster while the rest of the metadata assembly continues
    ds = ds.persist()

    # Determine time-invariance
    time_invariant = 'time' not in list(ds.coords.keys())

    # Assemble the context object (order dependent!)
    logger.debug('Assembling interpolation context.')
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

    # Sort the dimensions (fixes domain subsetting)
    logger.debug('Sorting data')
    sort_coords = list()
    for coord in 'time,lev,lat,lon'.split(','):
        if coord in ds.coords.keys():
            sort_coords.append(coord)

    ds = ds.sortby(sort_coords)

    logger.debug('Applying metadata schema')

    # Load a user-supplied schema, if one exists.
    if 'schema' in kwargs.keys():
        schema_key = kwargs['schema']
    else:
        schema_key = config['default_schema']

    schema = axs.load_schema(schema_key)
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

    # TODO: Need to find a less manual way to do this.
    for year in adu.generate_years_list(start_year, end_year):

        logger.info(f'Processing {year}')

        # Subset the data into just this year
        if not time_invariant:
            time_slice = slice(f'{year}-01-01', f'{year}-12-31')
            _ds = ds.sel(time=time_slice, drop=True)
        else:
            _ds = ds.copy()

        # Historical cutoff is defined in $HOME/.axiom/drs.json
        if config.enable_historical_cutoff == True:
            context['experiment'] = 'historical' if year < config.historical_cutoff else context['rcp']
        
        logger.info(f'Native frequency of data detected as {native_frequency}')

        # Automatically detect the output_frequency from the input data, this will not require resampling
        
        # Flag to trigger cell_method update below.
        resampling_applied = False

        if output_frequency == 'from_input' or output_frequency == native_frequency:
            output_frequency = adu.detect_input_frequency(_ds)
            logger.info(
                f'output_frequency detected from inputs ({output_frequency})')
            logger.info(f'No need to resample.')
            # Map the frequency to something DRS-compliant
            context['frequency_mapping'] = config['frequency_mapping'][output_frequency]

        # Fixed variables, just change the frequency_mapping
        elif adu.is_time_invariant(_ds):
            output_frequency = 'fx'
            logger.info(
                'Data is time-invariant (fixed variable), overriding frequency_mapping to fx')
            context['frequency_mapping'] = 'fx'

        # Actually perform the resample
        else:
            logger.debug(f'Resampling to {output_frequency} mean.')
            context['frequency_mapping'] = config['frequency_mapping'][output_frequency]
            _ds = _ds.resample(time=output_frequency, label='left').mean()

            # Update the cell methods below
            resampling_applied = True

        # Start persisting the computation now
        _ds = _ds.persist()

        # Monthly data should have the days truncated
        # context['start_date'] = f'{year}0101' if output_frequency[-1] != 'M' else f'{year}01'
        # context['end_date'] = f'{year}1231' if output_frequency[-1] != 'M' else f'{year}12'

        context['start_date'], context['end_date'] = adu.get_start_and_end_dates(year, output_frequency)

        # Tracking info
        context['creation_date'] = datetime.utcnow()
        context['uuid'] = uuid4()

        # Interpolate context
        logger.info('Interpolating context.')
        context = adu.interpolate_context(context)

        # Assemble the global meta, add axiom details
        logger.debug('Assembling global metadata.')
        global_attrs = dict(
            axiom_version=axiom_version,
            axiom_schema=schema_key
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

        # Copy coordinate attributes straight off the inputs
        if config.copy_coordinates_from_inputs:
            for coord in list(_ds.coords.keys()):
                _ds[coord].attrs = ds[coord].attrs

        # Assemble the encoding dictionaries (to ensure time units work!)
        logger.debug('Applying encoding')
        encoding = dict()

        for coord in list(_ds.coords.keys()):
            if coord not in config.encoding.keys():
                logger.warn(
                    f'Coordinate {coord} is not specified in drs.json file, omitting encoding.')
                continue
            encoding[coord] = config.encoding[coord]

        # Apply a blanket variable encoding.
        encoding[variable] = config.encoding['variables']

        # Postprocess data if required
        postprocessor = adu.load_postprocessor(postprocessor)

        def postprocess(_ds, *args, **kwargs):
            combined = dict()
            combined.update(kwargs)
            combined.update(local_args)
            combined['resampling_applied'] = resampling_applied
    
            return postprocessor(_ds, **combined)
        
        _ds = postprocess(_ds)

        # Update the cell methods
        if resampling_applied:
            _ds = update_cell_methods(_ds, variable, dim='time', method='mean')

        # Get the full output filepath with string interpolation
        logger.debug('Working out output paths')

        # Derive the start/end date strings from the actual timeseries and override
        if config.derive_filename_times_from_data:
            logger.info(
                'User has requested that filename times reflect the actual timeseries.')
            str_times = _ds.time.dt.strftime('%Y%m%d').data
            context['start_date'] = str_times[0]
            context['end_date'] = str_times[-1]
            logger.debug(
                'start_date = %(start_date)s, end_date = %(end_date)s' % context)

        drs_path = adu.get_template(config, 'drs_path') % context
        filename_template = adu.get_template(config, 'filename')

        # Override for fixed variables
        if adu.is_time_invariant(_ds):
            logger.debug('Overriding output filename template with fixed alternative.')
            filename_template = adu.get_template(config, 'filename_fixed')

        # Assemble the output filepath
        output_filename = filename_template % context
        output_filepath = os.path.join(
            output_directory, drs_path, output_filename)
        logger.debug(f'output_filepath = {output_filepath}')

        # Skip if already there and overwrite is not set, otherwise continue
        if os.path.isfile(output_filepath) and overwrite == False:
            logger.debug(
                f'{output_filepath} exists and overwrite is set to False, skipping.')
            continue

        # Check for uninterpolated keys in the output path, which should fail at this point.
        uninterpolated_keys = adu.get_uninterpolated_placeholders(
            output_filepath)

        if len(uninterpolated_keys) > 0:
            logger.error('Uninterpolated keys remain in the output filepath.')
            logger.error(f'output_filepath = {output_filepath}')
            raise DRSContextInterpolationException(uninterpolated_keys)

        # Create the output directory
        output_dir = os.path.dirname(output_filepath)
        logger.debug(f'Creating {output_dir}')
        os.makedirs(output_dir, exist_ok=True)

        # Supervise this job to ensure that it does in fact complete.
        with Supervisor(seconds=config.processing_timeout_seconds, error_msg=f'Variable {variable} took too long to complete, moving on.'):
            logger.info('Waiting for computations to finish.')
            progress(_ds)

        logger.debug(f'Writing {output_filepath}')
        write = _ds.to_netcdf(
            output_filepath,
            format='NETCDF4',
            encoding=encoding,
            unlimited_dims=['time']
        )

    elapsed_time = timer.stop()
    logger.info(f'DRS processing task took {elapsed_time} seconds.')


def load_variable_config(project_config):
    """Extract the variable configuration out of the project configuration.

    Args:
        project_config (dict-like): Project configuration.

    Returns:
        dict: Variable dictionary with name: [levels] (single level will have a list containing None.)
    """

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
    """Start a processing chain of multiple variables.

    Args:
        variables (list): List of variables to process.
        domain (str): Domain to process from domains.json.
        project (str): Project metadata to use from projects.json.
        **kwargs: Additional keyword arguments to pass to the processing chain.
    """

    logger = au.get_logger(__name__)

    # Load the project metadata
    project_config = load_config('projects')[project]
    config = load_config('drs')

    # Load all variables if nothing was supplied
    if not variables:

        # Select a default schema
        schema_key = config['default_schema']

        # Override if requested, this might be a direct filepath
        if 'schema_key' in kwargs.keys():
            schema_key = kwargs['schema']

        # Load it
        logger.info(f'No variables supplied, loading from schema as defined in configuration ({schema_key}).')
        schema = axs.load_schema(schema_key)        
        variables = list(schema['variables'].keys())
    
    
    else:
        logger.debug('User has supplied the following variables')
        logger.debug(variables)

    num_variables = len(variables)
    logger.info(f'{num_variables} variable(s) to process.')

    # Start the cluster if requested
    if config.dask['enable']:
        logger.info('Starting dask client.')

        cluster_config = config.dask['cluster']

        # Add PBS_JOBFS if set
        if 'PBS_JOBFS' in os.environ.keys():
            cluster_config['local_directory'] = os.getenv('PBS_JOBFS')

        cluster = LocalCluster(**cluster_config)
        client = Client(cluster)
        logger.info(client)

    output_frequencies = au.pluralise(kwargs['output_frequency'])
    
    # Yes this is a nested loop, but a single variable/domain/output_freq combination could still be 10K+ files, which WILL be processed in parallel.
    for variable in variables:
        for output_frequency in output_frequencies:

            attempt = 1
            no_files = False
            success = False

            rerun_attempts = config.get('rerun_attempts', default=1)

            while attempt <= rerun_attempts and no_files == False and success == False:

                logger.info(f'Processing {variable} {output_frequency}')
                instance_kwargs = kwargs.copy()
                instance_kwargs['variable'] = variable
                instance_kwargs['domain'] = domain
                instance_kwargs['project'] = project
                instance_kwargs['output_frequency'] = output_frequency

                if config.dask['enable']:
                    logger.info('Waiting for dask workers')
                    client.wait_for_workers(1, timeout=config['dask']['restart_timeout_seconds'])
                    logger.info(client)
                    logger.info(f'Dashboard located at {client.dashboard_link}')

                try:

                    process(**instance_kwargs)
                    success = True

                # Not technically an error, filtering has discounted all available files.
                except NoFilesToProcessException as ex:

                    logger.info(f'No files to process for {variable}')
                    no_files = True

                # Something wrong with the inputs regarding time.
                except IndexError as ex:

                    # Log the exception
                    log_exception(f'Timeseries inconsistency, check input data.', ex)

                    # Check recoverability
                    if is_error_recoverable(ex) and attempt <= rerun_attempts:
                        logger.info('Error is recoverable, incrementing attempts.')
                        attempt += 1
                        continue
                    
                    # Track the failure and max out the attempts to execute the finally clause
                    track_failure(variable, ex)
                    attempt = rerun_attempts + 1

                # Unknown exception
                except Exception as ex:
                    
                    log_exception(
                        f'Variable {variable} failed for output_frequency {output_frequency}. Error to follow',
                        ex
                    )

                    # Check recoverability
                    if is_error_recoverable(ex) and attempt <= rerun_attempts:
                        logger.info(
                            'Error is recoverable, incrementing attempts.')
                        attempt += 1
                        continue

                    # Track the failure and max out the attempts to execute the finally clause
                    track_failure(variable, ex)
                    attempt = rerun_attempts + 1

                # Run regardless of success/failure
                finally:
                    
                    if config.dask['enable'] and config.dask['restart_client_between_variables'] and no_files == False:
                        logger.info('User has requested dask client restarts between each variable (for resilience), restarting now.')
                        client.restart()
                        logger.info(client)

    logger.info('DRS processing complete, please see consumed/lock/log files for further detail.')


def filter_years(filepaths, year, offset=0):
    """Filter filepaths based on a year, plus or minus an offset.

    Args:
        filepaths (list): List of filepaths.
        year (int): Year.
        offset (int, optional): Number of years either side of YEAR to include. Defaults to 0.
    
    Returns:
        list : List of filtered filepaths.
    """
    _filepaths = list()
    years = range(year-offset, year+offset+1)
    for filepath in filepaths:
        for year in years:
            if str(year) in os.path.basename(filepath):
                _filepaths.append(filepath)
    
    return _filepaths


def update_cell_methods(ds, variable, dim='time', method='mean'):
    """Update the cell_methods attribute on the data.

    Args:
        ds (xarray.Dataset): Data.
        variable (str): Variable being processed.
        dim (str): Dimension over which method was applied.
        method (str): Method that was applied.
    """

    da = ds[variable]

    # If there is no cell_methods attribute, add it now.
    if 'cell_methods' not in da.attrs.keys():
        da.attrs['cell_methods'] = f'{dim}: {method}'

    # If the cell method was point, change it
    elif da.attrs['cell_methods'] == f'{dim}: point':
        da.attrs['cell_methods'] = f'{dim}: {method}'

    # If another operation was already applied and doesn't match this, append
    elif da.attrs['cell_methods'] != f'{dim}: {method}':
        da.attrs['cell_methods'] = da.attrs['cell_methods'] + f' {dim}: {method}'

    ds[variable] = da
    return ds


def log_exception(message, ex):
    """Log the exception.

    Args:
        message (str): Human-readable error message.
        ex (Exception): Stack trace.
    """
    logger = au.get_logger(__name__)
    logger.error(message)
    logger.exception(ex)


def is_error_recoverable(exception):
    """Check if the error is recoverable.

    Args:
        exception (Exception): Raised exception to check.
    
    Returns:
        bool : True if recoverable, False otherwise.
    """
    config = load_config('drs')
    return adu.is_error_recoverable(exception, config.get('recoverable_errors', list()))


def track_failure(variable, exception):
    """Track the failure for reprocessing.

    Args:
        variable (str): Variable name.
        exception (Exception): Exception raised.
    """
    
    config = load_config('drs')

    if config.track_failures and 'AXIOM_LOG_DIR' in os.environ.keys() and 'PBS_JOBNAME' in os.environ.keys():

        failed_filepath = os.path.join(
            os.getenv('AXIOM_LOG_DIR'),
            os.getenv('PBS_JOBNAME') + '.failed'
        )

        exname = type(exception).__name__

        with open(failed_filepath, 'a') as failed:  
            failed.write(f'{variable},{exname}\n')
