"""Code to perform DRS functions."""
from axiom.utilities import load_package_data
import argparse
import xarray as xr
import os
import uuid
from datetime import datetime
import sys
from pprint import pprint
import re


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
            frequencies = ['day', 'month']
            
            if variable_fixed:
                frequencies = ['day']
        
        if no_frequencies:
            frequencies = ['3hr', 'day', 'month']

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
        frequencies = ['day', 'month']

        if variable_fixed:
            frequencies = ['day']
    
    # Return everything
    return dict(
        domains=domains,
        frequencies=frequencies,
        resolution_dir=resolution_dir,
        degrees=degrees
    )


def get_rundir():
    """Get the run directory from either PBS or pwd.

    Returns:
        str : Run directory.
    """
    if 'PBS_O_WORKDIR' in os.environ.keys():
        return os.getenv('PBS_O_WORKDIR')

    return os.getcwd()


def get_decades(start_year, end_year):
    """Get a list of decades to process (usually only 1)

    Args:
        start_year (int) : Start year.
        end_year (int) : End year.

    Returns:
        list : List of decades to process.
    """
    
    # Convert to decades
    start_decade = start_year // 10
    end_decade = end_year // 10

    # Degenerate case, just do a single decade
    if start_decade == end_decade:
        return [start_decade]

    return range(start_decade, end_decade + 1)


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


def get_lengths(decade, s_year, e_year, context=None):
    """Get the number of times expected given a decade.

    Args:
        decade (int) : Decade.
    """

    # Overrides for specific conditions
    if decade == 200:

        if s_year == 2006:
            e_year = 2009
        else:
            s_year = 2000

            if context['gcm_model'] == 'ERAINT':
                e_year = (decade * 10) + 9
            else:
                e_year = 2005
    
    elif context['gcm_model'] == 'ERAINT' and decade == 201:
        e_year = (decade * 10) + 4
    else:
        s_year = decade * 10
        e_year = (decade * 10) + 9

    
    dt =  datetime(e_year, 12, 31) - datetime(s_year, 1, 1)

    # Work out the expected lengths at different resolutions
    num_seconds = dt.total_seconds()
    num_minutes = num_seconds / 60
    num_1hr = num_minutes / 60
    num_12min = num_1hr * 5
    num_days = num_1hr / 24
    num_3hrs = num_days * 8
    num_months = ((e_year - s_year) + 1) * 12

    # Return everything
    return {
        'num_month': num_months,
        'num_day': num_days,
        'num_3hr': num_3hrs,
        'num_1hr': num_1hr,
        'num_12min': num_12min,
        'num_minutes': num_minutes,
        'num_seconds': num_seconds,
        's_year': s_year,
        'e_year': e_year,
    }


def get_lengths_new(decade, **context):
    """Get lengths.

    Args:
        **context : Context dictionary, because there are a lot of variables.
    
    Returns:
        dict : Dictionary of lengths
    """
    s_year, e_year = context['s_year'], context['e_year']
    even = decade % 2 == 0
    single_year = s_year == e_year

    lengths = dict()

    if single_year:
        
        lengths['num_month'] = 12
        lengths['num_day'] = 365

        # Check leap year
        if s_year % 4 == 0:
            lengths['num_day'] = 366

    else:

        if even:

            # There is an extra leap year in these decades
            lengths['num_day'] = 3653
            lengths['num_1hr'] = 87672

        else:

            lengths['num_day'] = 3652
            lengths['num_1hr'] = 87648
        
        if decade == 200:

            if s_year == 2006:

                e_year = 2009
                lengths['num_month'] = 48
                lengths['num_day'] = 1461
                lengths['num_1hr'] = 35064
            
            else:

                s_year = 2000

                if context['gcm_model'] == 'ERAINT':
                    
                    e_year = (decade * 10) + 9
                
                else:

                    e_year = 2005
                    lengths['num_month'] = 72
                    lengths['num_day'] = 2192
                    lengths['num_1hr'] = 52608
        
        elif context['gcm_model'] == 'ERAINT' and decade == 201:

            e_year = (decade * 10) + 4
            lengths['num_month'] = 60
            lengths['num_day'] = 1826
            lengths['num_1hr'] = 43824

        else:

            s_year = decade * 10
            e_year = (decade * 10) + 9
        
        lengths['num_12min'] = lengths['num_1hr'] * 5
        lengths['num_3hr'] = lengths['num_day'] * 8
    
    # Copy in the new year info
    lengths['s_year'] = s_year
    lengths['e_year'] = e_year
    return lengths


def detect_resolution(path):
    """Attempt to detect the domain information from the path.

    Args:
        path (str): Path.
    
    Returns:
        int : Domain
    
    Raises:
        ValueError : When the domain can't be detected.
    """
    matches = re.findall(r'[0-9]*km', path)

    num_matches = len(matches)
    if num_matches != 1:
        raise ValueError(f'Unable to detect domain in {path}. There should be exactly 1 domain segment in the path, found {num_matches}')
    
    return int(matches[0].replace('km', ''))
    


# if __name__ == '__main__':
def test():

    # Load configuration
    drs_config = load_package_data('data/drs.json')

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.description = 'This script post-processes output from CCAM runs into DRS format.'

    parser.add_argument('input_dir', type=str, help='Input directory.')
    parser.add_argument('output_dir', type=str, help='Input directory.')

    # parser.add_argument('odir', type=str, help='Output directory.')
    # parser.add_argument('base', type=str, help='Filename base.')
    parser.add_argument('model', type=str, help='GCM name.')
    parser.add_argument('start_year', type=int, help='Start year.')
    parser.add_argument('end_year', type=int, help='End year.')
    parser.add_argument('ivar', type=str, help='Variable name.')
    parser.add_argument('freq', type=str, help='Output frequency')
    parser.add_argument('project', type=str, help='Funding project.')
    args = parser.parse_args()
    
    # Set up context - starting with the arguments supplied to the script
    # We do things this way because there is a lot of string interpolation
    context = vars(args)

    # Add the defaults
    context.update(drs_config['defaults'])

    # Check if this is a fixed variable
    context['fixed'] = context['ivar'] in drs_config['fixed_variables']
    if context['fixed']:
        context['ensemble'] = 'r0i0p0'

    context['var_in'] = context['ivar']
    context['var_out'] = context['ivar']

    # Get directories and create output location
    context['rundir'] = get_rundir()

    # Create a list of decades to process (usually 1)
    decades = get_decades(args.start_year, args.end_year+1)

    # Detect the resolution from the path information
    context['res_km'] = detect_resolution(args.input_dir)

    # Work out the frequencies
    if context['freq']:
        context['freqs'] = [context['freq']]
        context['no_freq'] = False
    else:
        context['freqs'] = '1hr,day,month'.split(',')
        context['no_freq'] = True

    n_domains = 1

    # Set up the domain information
    domains = get_domains(context['res_km'], args.freq, context['fixed'], context['no_freq'])

    context['run_type'] = 'Climate change'
    context['mode'] = ' bias- and variance-corrected sea surface temperatures'

    # Load the metadata for this project
    meta_model = drs_config['metadata'][args.model]

    meta_model['gcm_model'] = meta_model['gcm_model'] % dict(model=args.model)

    meta_project = drs_config['projects'][args.project]

    # Add the meta to the context, there is some info there that will be needed.
    context.update(meta_model)
    context.update(meta_project)

    # TODO: This could be replaced with a better set of defaults to save conditionals
    # ~L297 in procvar4
    if context['freq'] == 'cordex':

        meta_project['project_combined'] = 'CORDEX'

    elif meta_model['gcm_model'] == 'ERAINT':

        meta_project['project_long'] = "2018 Australian Wine Industry Projections and 2019 DELWP Victorian Climate Projections"
        meta_project['plural'] = 's'
        meta_project['project_combined'] = 'WINE / DELWP'

    else:

        meta_project['plural'] = 's'
        meta_project['project_combined'] = args.project

    # Set starting defaults for start/end years
    context['s_year'] = context['start_year']
    context['e_year'] = context['end_year']

    for decade in decades:

        for file_freq in context['freqs']:
            
            # Update the path freq
            if file_freq == 'fixed':
                path_freq = 'fx'
            elif file_freq == 'month':
                path_freq = 'mon'
            else:
                path_freq = file_freq

            context['path_freq'] = path_freq

            pprint(context)
            sys.exit()
                
            # for domain in domains:


    # # Loop through each decade
    # for decade in decades:

    #     lengths, s_year, e_year = get_lengths_new(decade, **context)

    #     # Update with what has been calculated
    #     context.update(lengths)

    #     # Set these in the context
    #     context['s_year'] = s_year
    #     context['e_year'] = e_year

    #     # Change the formatting depending on the frequency
    #     if freq
    #     start_date = datetime(s_year, 1, 1).strftime('%')
    #     end_date = datetime(e_year, 1, 1)


    #     # Override for 2006
    #     if context['s_year'] < 2006:
    #         context['experiment'] = 'historical'
    #     else:
    #         context['experiment'] = context['rcp']
        
    #     # Override for ERA
    #     if context['gcm_model'] in ['ERAINT', 'ERA5']:
    #         context['experiment'] = 'evaluation'
    #         context['description'] = get_template(drs_config, "description_era") % context
    #     else:
    #         context['description'] = get_template(drs_config, "description_other") % context

    #     # Loop through each domain requested
    #     for domain in domains['domains']:

    #         # Load the domain info from the config (dx etc.)
    #         domain_info = drs_config['domains'][domain]
    #         context['domain'] = domain
    #         context.update(domain_info)

    #         # Special case
    #         if domain == 'AUS-44i':
                
    #             drs_project = 'CORDEX'
                
    #             if file_freq == 'month':
    #                 start_mmdd ='01'
    #                 end_mmdd = '12'

    #         # Interpolate the filename information
    #         if context['fixed']:
    #             context['filename_template'] = get_template(drs_config, 'filename_fixed')
    #         else:
    #             context['filename_template'] = get_template(drs_config, 'filename')

    #         # Build output path
    #         context['drs_base'] = get_template(drs_config, 'drs_base') % context
    #         output_path_template = get_template(drs_config, 'drs_path')
    #         output_path = output_path_template % context
    #         context['output_path_template'] = output_path_template
    #         context['output_path'] = output_path

    #         # Build output filename and put them together
    #         output_filename = get_template(drs_config, 'filename') % context
    #         output_filepath = os.path.join(output_path, output_filename)

    #         # TODO: List of global attributes to retain?
    #         pprint(context)
    #         sys.exit()

    #         # Open the work file
    #         ds = xr.open_dataset(input_filename)

    #         # Find the date the model was run
    #         creation_date = ds.attrs['date_header']

    #         # Get the number of x-y and z grid points of the model grid
    #         il = ds.attrs['il']
    #         kl = ds.attrs['kl']
    #         rlon = ds.attrs['rlong0']
    #         rlat = ds.attrs['rlat0']

    #         # Get grid stretching value of model grid
    #         schmidt = ds.attrs['schmidt']

    #         # Set created date
    #         created = datetime.utcnow()
    #         metadata_mod_date = datetime.utcnow()

    #         # Create a unique identifier for this file
    #         uuid = uuid.uuid4()

    #         # Select domain
    #         # ds = ds.sel()

    #         # Get dimension information
    #         geospatial_lon_min = ds.lon.min()
    #         geospatial_lon_max = ds.lon.max()
    #         geospatial_lat_min = ds.lat.min()
    #         geospatial_lat_max = ds.lat.max()

    #         # Get vertical for 3D data
    #         if 'lev' in ds.coords.keys():
    #             geospatial_vertical_min = ds.lev.min()
    #             geospatial_vertical_max = ds.lev.max()
    #             geospatial_vertical_units = ds.lev.attrs['units']
            
    #         # Skip if dry run
    #         if dry_run:
    #             continue

    #         # Strip global attributes
    #         ds.attrs = dict()

    #         # Read the expected variable metadata out of the spec
    #         # TODO: Allow day or month here (also fixed)
    #         metadata_spec = 'cordex-day.csv'
    #         metadata = load_package_data(f'specifications/{metadata_spec}')
    #         metadata = metadata['variables'][args.ivar]

    #         metadata_updates = dict()

    #         # Extract the 
    #         for attr, schema in metadata.items():
    #             metadata_updates[attr] = schema['allowed'][0]
                
    #         ds[ivar].attrs.update(metadata_updates)

    #         # TODO: Check that the file is of the correct length
    #         num_times = ds.time.shape[0]
            
    #         if num_times != expected_times:
    #             print('ERROR: Newly created file is of wrong, nonzero length')
    #             print(f'Expected {expected_times}, found {num_times}')
            
    #         # Construct the global metadata from all of the logic above
    #         meta_global = dict()

    #         # Put the metadata together from different sources

    #         for meta in [model_metadata, project_metadata]:

    #             # Then individual attributes
    #             for key, value in meta.items():

    #                 # Interpolate the context into the values and assign to the final
    #                 meta_global[key] = value % context

    #         # Assign the metadata to the file
    #         ds.attrs.update(meta_global)

    #         # TODO: Convert to netcdf4 classic and save
    #         os.makedirs(output_path, exist_ok=True)
    #         ds.to_netcdf(output_filepath, format='NETCDF4_CLASSIC')

def get_model_meta(config, model, is_cordex=False):
    """Get the model metadata out of the config.

    Args:
        config (dict): Configuration dictionary.
        model (str): Model name
        is_cordex (bool, optional): Apply cordex overrides. Defaults to False.

    Returns:
        dict: Model metadata.
    """

    meta = config['models'][model]

    # Override with cordex alternatives
    if is_cordex and '_cordex' in meta.keys():
        meta.update(meta['_cordex'])
        meta.pop('_cordex', None)

    return meta


def get_project_meta(config, project, is_cordex=False, is_eraint=False):

    meta = config['projects'][project]

    if is_cordex:

        meta['project_combined'] = 'CORDEX'

    else:

        if is_eraint:

            meta['project_long'] = "2018 Australian Wine Industry Projections and 2019 DELWP Victorian Climate Projections"
            meta['plural'] = 's'
            meta['project_combined'] = 'WINE / DELWP'

        else:

            meta['plural'] = 's'
            meta['project_combined'] = project
    
    return meta

if __name__ == '__main__':

    config = load_package_data('data/drs.json')

    parser = argparse.ArgumentParser()
    parser.description = 'DRS processing script'
    parser.add_argument('input_dir')
    parser.add_argument('output_dir')
    parser.add_argument('-s', '--start_year', required=True, type=int)
    parser.add_argument('-e', '--end_year', required=True, type=int)
    parser.add_argument('-v', '--variable', required=True, type=str)
    parser.add_argument('-f', '--frequency', type=str)
    parser.add_argument('-p', '--project', required=True, type=str)
    parser.add_argument('-m', '--model', required=True, type=str)
    parser.add_argument('-b', '--base', required=True, type=str)
    
    # Initialise context, add defaults
    context = config['defaults']
    context.update(vars(parser.parse_args()))

    # Check if the variable is fixed
    is_fixed = context['variable'] in config['fixed_variables']

    # Get a list of decades
    decades = get_decades(context['start_year'], context['end_year'])
    
    # Detect resolution
    res_km = detect_resolution(context['input_dir'])
    context['res_km'] = res_km

    # Get frequencies
    no_frequency = context['frequency'] is None
    is_cordex = context['frequency'] == 'cordex'

    if no_frequency:
        frequencies = '1hr,day,month'.split(',')
    else:
        frequencies = [context['frequency']]

    context['frequencies'] = frequencies

    # Get domain information
    domain_info = get_domains(
        res_km,
        context['frequency'],
        is_fixed,
        no_frequency
    )

    context.update(domain_info)
    domains = domain_info['domains']

    # Get model information
    meta_model = get_model_meta(config, context['model'])
    context.update(meta_model)
    is_eraint = meta_model['gcm_model'] == 'ERAINT'

    # Get project information
    meta_project = get_project_meta(
        config,
        context['project'],
        is_cordex=is_cordex,
        is_eraint=is_eraint   
    )

    context.update(meta_project)

    context['s_year'] = context['start_year']
    context['e_year'] = context['end_year']

    for decade in decades:

        # Update length information
        length_info = get_lengths_new(decade, **context)
        context.update(length_info)

        # TODO: Need a cleaner way to do this
        if context['s_year'] < 2006:
            context['experiment'] = 'historical'
        else:
            context['experiment'] = context['rcp']
        
        if context['gcm_model'] in ['ERAINT', 'ERA5']:
            context['experiment'] = 'evaluation'
            description_template = 'description_era'
        else:
            description_template = 'description_other'

        context['description'] = get_template(config, description_template) % context

        pprint(context)
        sys.exit()

        for frequency in frequencies:
            for domain in domains:
                print(decade, frequency, domain)

    # if context['freq'] == 'cordex':

    #     meta_project['project_combined'] = 'CORDEX'

    # elif meta_model['gcm_model'] == 'ERAINT':

    #     meta_project['project_long'] = "2018 Australian Wine Industry Projections and 2019 DELWP Victorian Climate Projections"
    #     meta_project['plural'] = 's'
    #     meta_project['project_combined'] = 'WINE / DELWP'

    # else:

    #     meta_project['plural'] = 's'
    #     meta_project['project_combined'] = args.project

    
    pprint(context)