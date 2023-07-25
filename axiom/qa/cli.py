"""Command-line methods for the QA subsystem."""
import os
import sys
import argparse
import axiom.schemas as axs
import axiom.qa as axq
from axiom.config import load_config
import axiom.drs.utilities as adu
import axiom.utilities as au
from axiom.drs.payload import Payload
from blush import parallelise, unpack_results
import numpy as np
import pandas as pd
import xarray as xr


def qa_timeseries(path, schema, start_year, end_year, output_filepath, errors=False):
    """Scan the timeseries for missing files.
    
    Args:
        path (str): Globbable path with {variable} and {year} placeholders.
        schema (str): Schema name or path.
        start_year (int): Start year.
        end_year (int): End year.
        output_filepath (str): Path to which to write the report.
        errors (bool, Optional): Report only on errors. Defaults to False.  
    """
    
    # Load the schema
    _schema = axs.load_schema(schema)
    variables = list(_schema['variables'].keys())

    print('Checking timeseries...')
    df = axq.check_timeseries_variable(path, variables, start_year, end_year)

    # Filter only for errors.
    if errors:
        df = df[df['status'] == 'ERROR']

    # Write the report
    output_filepath = os.path.abspath(output_filepath)
    df.to_csv(output_filepath, index=False)
    
    print(f'Report available at {output_filepath}')


def qc(schema, payload, start_year, end_year, report_dir=None, nstd=2.0, pct_mean=0.75, checks='nan,nstd,pct_mean', ignore_missing_inputs=False, create_payloads=False):
    """Run Quality-Control.

    Args:
        schema (str): Schema key or filepath used in processing.
        payload (str): Path to one of the payloads used in processing.
        start_year (int): First year of data.
        end_year (int): Last year of data.
        report_dir (str, optional): Path to which to write report files. Defaults to None.
        nstd (float, optional): Number of standard deviations out to consider anomalous. Defaults to 2.0.
        pct_mean (float, optional): Threshold percentage of mean file size to consider anomalous. Defaults to 0.75.
        checks (str, optional): Comma-separated checks to apply. Defaults to 'nan,nstd,pct_mean'.
        ignore_missing_inputs (bool, optional): Ignore variables that are missing from the input directory, requires that directory still exists. Defaults to False.
    """

    # Break up the checks for evaluation later
    checks = set(sorted(checks.split(',')))

    # Load the configuration, get the templates for filepaths
    config = load_config('drs')
    logger = au.get_logger(__name__)

    # Start building context
    context = config.get('metadata_defaults')
    
    drs_template = adu.get_template(config, 'drs_path')
    filename_template = adu.get_template(config, 'filename')

    # Load the schema
    _schema = axs.load_schema(schema)

    # Load the payload, update the context
    _payload = Payload.from_json(payload)
    context.update(_payload.to_dict())

    context['gcm_institute'] = '*'
    context['gcm_model'] = '*'
    context['rcm_model'] = '*'

    context['frequency_mapping'] = config.get('frequency_mapping')[_payload.output_frequency]

    expected_filepaths = list()
    variables = list()
    years = list()

    # Create a list of variables to check
    variables2check = _schema['variables'].keys()
    variables2ignore = list()

    # Get a list of variables that are in the input directory to exclude.
    input_variables = list()

    logger.info('Assembling a list of variables to check.')

    if ignore_missing_inputs:

        logger.info('User has requested missing inputs be ignored')

        input_filepaths = au.auto_glob(_payload.input_files)
        if len(input_filepaths) == 0:
            logger.error('Inputs no longer exist! Unable to proceed with QC using --ignore_missing_inputs')
            raise FileNotFoundError('Unable to collect a list of variables from payload inputs, does the directory still exist?')
        
        for input_filepath in input_filepaths:
            
            input_variable = os.path.basename(input_filepath).split('_')[0]
            
            # Skip variables that have previously been ignored / added.
            if input_variable in variables2ignore or input_variable in input_variables:
                continue

            # On the first instance, check if it is not expected due to frequency
            if input_variable not in input_variables:

                logger.info(f'Checking if we should test for {input_variable}')
                
                ds = xr.open_dataset(input_filepath, chunks=dict(time=1))
                freq = adu.detect_input_frequency(ds)
            
                if freq != _payload.output_frequency and config.allow_subdaily_resampling == False:
                    logger.info(f'{input_variable} is on a different frequency and allow_subdaily_resampling is disabled. Ignoring')
                    variables2ignore.append(input_variable)
                    continue

            logger.info(f'Adding {input_variable} to the list to be checked.')                    
            input_variables.append(input_variable)

        # Perform an intersection to get the true list of variables to check.
        variables2check = list(set(variables2check) & set(input_variables))

    # Loop through all of the variables
    for variable in variables2check:
        
        for year in range(start_year, end_year+1):

            context['variable'] = variable
            context['start_date'], context['end_date'] = adu.get_start_and_end_dates(year, _payload.output_frequency)
            drs_path = drs_template % context
            filename = filename_template % context
            expected_filepath = os.path.join(drs_path, filename)
            expected_filepaths.append(expected_filepath)
            variables.append(variable)
            years.append(year)
    
    # Check all of the files in parallel
    logger.info(f'Checking timeseries...')
    results = parallelise(_check_file, num_threads=8, filepath=expected_filepaths, year=years, variable=variables)
    results = unpack_results(results)

    # Assemble a dataframe for aggregate statistics
    df = pd.DataFrame(results)

    df_nstd = None
    df_nan = None
    df_pct_mean = None

    # Check the results
    for variable in df.variable.unique().tolist():

        # Extract just this variable
        var_df = df[df.variable == variable]

        # Do nan check
        if 'nan' in checks:
            _df_nan = _filter_nan(var_df)
            df_nan = _df_nan if df_nan is None else pd.concat([df_nan, _df_nan])

        # Do nstd check
        if 'nstd' in checks:
            _df_nstd = _filter_nstd(var_df, nstd)
            df_nstd = _df_nstd if df_nstd is None else pd.concat([df_nstd, _df_nstd])
        
        # Do pct_mean check
        if 'pct_mean' in checks:
            _df_pct_mean = _filter_pct_mean(var_df, threshold=pct_mean)
            df_pct_mean = _df_pct_mean if df_pct_mean is None else pd.concat([df_pct_mean, _df_pct_mean])


    # Check if there are any errors to report
    num_nan = len(df_nan.index) if df_nan is not None else 0
    num_nstd = len(df_nstd.index) if df_nstd is not None else 0
    num_pct_mean = len(df_pct_mean.index) if df_pct_mean is not None else 0

    # Take the sum of the entries for the error check
    is_error = num_nan + num_nstd + num_pct_mean > 0

    # Exit with status
    if is_error:
        logger.error('Timeseries has failed QC')
        logger.error(f'{num_nan} Files missing/no filesize = {num_nan}')
        logger.error(f'{num_nstd} files are more than {nstd} away from the mean file size.')
        logger.error(f'{num_pct_mean} files are less than {pct_mean} from the mean file size.')
    
    # Write the reports at the given directory
    if report_dir and is_error:
        os.makedirs(report_dir, exist_ok=True)

        if 'nstd' in checks:
            df_nstd.to_csv(os.path.join(report_dir, 'errors_nstd.csv'))
        
        if 'nan' in checks:
            df_nan.to_csv(os.path.join(report_dir, 'errors_nan.csv'))
        
        if 'pct_mean' in checks:
            df_pct_mean.to_csv(os.path.join(report_dir, 'errors_pct_mean.csv'))
        
        logger.error(f'Reports written to {report_dir}')

    # Create payloads to rerun for the errors
    if create_payloads and is_error:

        for year, group_df in df_nan.groupby('year'):

            rerun_payload = _payload
            rerun_payload.start_year = year
            rerun_payload.end_year = year
            rerun_payload.variables = group_df.variable.unique().tolist()

            # print(rerun_payload.to_dict())
            payload_filepath = os.path.join(
                report_dir,
                rerun_payload.get_filename()
            )

            rerun_payload.to_json(payload_filepath)

    sys.exit(int(is_error))


def _filter_nstd(df, nstd):
    """Check the filesize for anomalies that exceed nstd from the mean.

    Args:
        df (pandas.Dataframe): Dataframe with a 'size' column.
        nstd (float): Number of standard deviations to consider.
    
    Returns:
        pandas.Dataframe: Dataframe filtered by those that exceed nstd, with additional column 'nstd_from_mean_size'.
    """
    _mean = df['size'].mean()
    _std = df['size'].std()

    # Get the distance away from the mean, check the threshold
    df['nstd_from_mean'] = (df['size'] - _mean) / _std
    df = df[(np.abs(df['nstd_from_mean_size']) > nstd)]

    return df


def _filter_pct_mean(df, threshold):
    """Filter the files for those whose size is less that threshold of the mean.

    Args:
        df (pandas.Dataframe): Dataframe with a 'size' column.
        threshold (float): Fraction to below which will constitute an anomaly.
    
    Returns:
        pandas.Dataframe: Dataframe filtered by anomalies less than the fractional threshold supplied, with additional column 'pct_mean_size'.
    """
    _mean = df['size'].mean()
    df['pct_mean_size'] = df['size'] / _mean
    df = df[df.pct_mean_size <= threshold]
    return df


def _filter_nan(df):
    """Filter the files for those which have NaN for their filesize.

    Args:
        df (pandas.Dataframe): Dataframe with a 'size' column.
        threshold (float): Fraction to below which will constitute an anomaly.
    
    Returns:
        pandas.Dataframe: Dataframe filtered by anomalies less than the fractional threshold supplied, with additional column 'pct_mean_size'.
    """
    return df[df['size'].isnull()]


def _check_file(filepath, year, variable):

    found_files = au.auto_glob(filepath)

    result = dict(year=year, variable=variable)

    # Check if the variable is actually fixed?
    fixed_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(filepath))),
        'fx',
        variable,
        f'{variable}_*.nc'
    )

    found_fixed_files = au.auto_glob(fixed_path)

    if not found_files and not found_fixed_files:
        result.update(dict(filepath=filepath, size=np.nan))
        return result
    
    if found_files:
        _filepath = found_files[0]
    elif found_fixed_files:
        _filepath = found_fixed_files[0]

    if not _filepath:
        result.update(dict(filepath=_filepath, size=np.nan))
        return result
    
    size = os.path.getsize(_filepath)
    result.update(dict(filepath=_filepath, size=size))
    return result


def get_parser(config=None, parent=None):
    """Parse arguments for the QA module.

    Args:
        config (dict, optional): Configuration dictionary. Defaults to None.
        parent (obj, optional): Parent parser object. Defaults to None.
    
    Returns:
        argparse.Namespace: Arguments object.
    """
    if parent is None:
        parser = argparse.ArgumentParser()
    else:
        parser = parent.add_parser('drs_qc')
    
    parser.add_argument('schema', type=str, help='Schema name or path that was used to process data.')
    parser.add_argument('payload', type=str, help='Example payload that was used to process data.')
    parser.add_argument('start_year', type=int, help='Start year.')
    parser.add_argument('end_year', type=int, help='End year.')
    parser.add_argument('--report_dir', type=str, help='Optional directory in which to place error reports.', default=None)
    parser.add_argument('--nstd', type=float, help='Number of standard deviations out to consider an error. (Default = 2.0)', default=2.0)
    parser.add_argument('--pct_mean', type=float, help='Percentage of mean file size out to consider an error (fraction). (Default = 0.75)', default=0.75)
    parser.add_argument('--checks', type=str, help='Checks to run. Defaults to "nan,nstd,pct_mean"', default='nan,std,pct_mean')
    parser.add_argument('--ignore_missing_inputs', help='Ignore variables that are not found in the input directory (requires that directory still exist!)', action='store_true', default=False)
    parser.add_argument('--create_payloads', help='Create payloads to rerun for the different errors.', action='store_true', default=False)
    parser.set_defaults(func=qc)
    
    return parser
