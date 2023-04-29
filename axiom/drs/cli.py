"""Command-line methods for the DRS subsystem."""
from ast import arg
import os
import sys
import json
import argparse
import axiom.utilities as au
from axiom.config import load_config
import axiom.drs.payload as adp
import axiom.drs.utilities as adu
from tqdm import tqdm
from pathlib import Path
import shutil
import datetime


def split_args(values):
    """Split an argument that is comma-separated.

    Args:
        values (str): Values
    
    Returns:
        list : List of split arguments.
    """
    return values.split(',')


def get_parser(config=None, parent=None):
    """Parse arguments for command line utiltities.

    Args:
        config (dict) : Configuration dictionary.
        parent (obj) : Parent parser object (for integration into the main axiom CLI)

    Returns:
        argparse.Namespace : Arguments object.
    """
    
    # Load the model, project and domain
    VALID_MODELS = load_config('models').keys()
    VALID_PROJECTS = load_config('projects').keys()
    VALID_DOMAINS = load_config('domains').keys()

    # Build a parser
    if parent is None:
        parser = argparse.ArgumentParser()
    # ...or add one to the top-level CLI
    else:
        parser = parent.add_parser('drs')

    parser.description = "DRS utility"

    # Paths
    parser.add_argument('--input_files', required=True, type=str, help='Input filepaths', nargs=argparse.ONE_OR_MORE)
    parser.add_argument('--output_directory', required=True, type=str, help='Output base directory (DRS structure built from here)')
    parser.add_argument('-o', '--overwrite', default=False, help='Overwrite existing output', action='store_true')

    # Temporal
    parser.add_argument('-s', '--start_year', required=True, type=int, help='Start year')
    parser.add_argument('-e', '--end_year', required=True, type=int, help='End year')

    # Resolution and output frequency
    parser.add_argument('-r', '--input_resolution', type=float, help='Input resolution in km, leave blank to auto-detect from path.')
    
    parser.add_argument(
        '-f', '--output_frequency', required=True, type=str, metavar='output_frequency',
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
        help='Domain to process'
    )

    # Override the variables defined in the drs.json file
    parser.add_argument(
        '-v', '--variable',
        type=str,
        required=True,
        help='Variable to process.'
    )

    return parser


def get_parser_consume(config=None, parent=None):
    """A parser that can consume JSON payload files.

    Args:
        config ([type], optional): [description]. Defaults to None.
        parent ([type], optional): [description]. Defaults to None.
    """
    if parent is None:
        parser = argparse.ArgumentParser()
    else:
        parser = parent.add_parser('drs_consume')

    # Input filepaths
    parser.add_argument('input_filepaths', type=str, help='Input json filepaths.', nargs=argparse.ONE_OR_MORE)
    return parser


def drs_launch(path, jobscript, log_dir, batches=None, dry_run=True, interactive=False, unlock=False, **launch_context):
    """Method to launch a series of qsubs for DRS processing.

    Args:
        path (str): Globbable path of payload files.
        jobscript (str): Path to the job script.
        log_dir (str): Path to which to save the log files.
        batches (int): Number of batches to split variables into (for parallel processing).
        dry_run (bool): Print out the commands rather than executing.
        interactive (bool): Dump the interactive flag into the qsub command when dumping.
        unlock (bool): Unlock locked payloads prior to submission (for rerunning walltime overruns)
        **launch_context: Additional arguments that will be interpolated as launch context.
    """

    # List the payloads in the input_directory
    payloads = au.auto_glob(path)

    if not dry_run:
        os.makedirs(log_dir, exist_ok=True)

    for payload in payloads:

        # Load the payload to get the project, where we can get the variables and work out what the batch_size will be
        if batches:
            payload_obj = json.load(open(payload, 'r'))
            project = payload_obj['project']
            project_config = load_config('projects')[project]
            variables = project_config['variables_2d'] + list(project_config['variables_3d'].keys())
            variables_batches = au.batch_split(variables, n_batches=batches)
            _batches = range(1, len(variables_batches) + 1)
            
        
        # Single batch, which makes the next section simpler
        else:
            _batches = [1]

        # Unlock the file if requested
        if au.is_locked(payload) and unlock == True:
            print(f'Unlocking {payload} for resubmission')
            au.unlock(payload)
        
        # Skip if not
        elif au.is_locked(payload):
            print(f'{payload} is locked.')
            continue

        # Convert the path to the jobscript to an absolute path for reproducibility
        jobscript = os.path.abspath(jobscript)
        payload = os.path.abspath(payload)
        log_dir = os.path.abspath(log_dir)

        for batch_id in _batches:

            job_name = os.path.basename(payload)
            batch_str = str(batch_id).zfill(3)
            job_name = f'{job_name}_{batch_str}'

            # Assemble the command from configuration
            config = load_config('drs')
            directives = config['launch']['directives']

            # Add interactive flag when dry running
            if dry_run and interactive:
                directives.append('-I')

            # Override walltime
            if 'walltime' in launch_context.keys() and launch_context['walltime'] is not None:
                walltime = launch_context['walltime']
                directives.append(f'-l walltime={walltime}')

            qsub_vars = dict(
                AXIOM_PAYLOAD=payload,
                AXIOM_LOG_DIR=log_dir,
                AXIOM_BATCH=batch_id
            )

            # Assemble the launch context for this job
            _launch_context = dict(
                qsub_vars=adu.assemble_qsub_vars(**qsub_vars),
                job_name=job_name,
                log_dir=log_dir,
                batch_str=batch_str
            )

            # Add this to the user-supplied launch context
            launch_context.update(_launch_context)

            # Assemble the qsub command
            cmd = adu.assemble_qsub_command(
                jobscript=jobscript,
                directives=directives,
                **launch_context
            )

            # Dry run, just echo the outputs
            if dry_run:
                print(cmd)
        
            # Real run, submit the jobs.
            else:
                qsub = au.shell(cmd)
                if qsub.returncode == 0:
                    print(qsub.stdout.decode('utf-8'))


def get_parser_launch(parent=None):
    """A parser that can launch DRS processing.

    Args:
        parent ([type], optional): [description]. Defaults to None.
    """
    if parent is None:
        parser = argparse.ArgumentParser()
    else:
        parser = parent.add_parser('drs_launch')

    parser.description = 'Submit drs_consume tasks via qsub.'

    # Input filepaths
    parser.add_argument('path', type=str, help='Globbable path to payload files (use quotes)')
    parser.add_argument('jobscript', type=str, help='Path to the jobscript for submission.')
    parser.add_argument('log_dir', type=str, help='Directory to which to write logs.')
    parser.add_argument('-d', '--dry_run', action='store_true', default=False, help='Print commands without executing.')
    parser.add_argument('-i', '--interactive', action='store_true', default=False, help='Dump the interactive flag into the qsub command when dry-running.')
    parser.add_argument('--walltime', type=str, help='Override walltime in job script.')
    parser.add_argument('--unlock', help='Unlock locked payloads prior to submission', action='store_true', default=False)
    parser.set_defaults(func=drs_launch)

    return parser


def generate_payloads(payload_dst, input_files, output_dir, start_year, end_year, project, model, domain, variables=None, schema=None, output_frequencies='1H,6H,1D,1M', num_batches=1, extra=None):
    
    # Unpack the extra arguments
    _extra = dict()
    for kv in extra:
        k, v = kv.split(',')
        _extra[k] = v

    payloads = adp.generate_payloads(
        input_files=input_files,
        output_directory=output_dir,
        start_year=start_year, end_year=end_year,
        project=project, model=model, domain=domain,
        variables=variables, schema=schema,
        output_frequencies=output_frequencies,
        num_batches=num_batches,
        **_extra
    )

    if len(payloads) == 0:
        raise Exception('No payloads generated!!!')

    # Create the output directory
    os.makedirs(payload_dst, exist_ok=True)

    # Generate the payloads at the destination
    print('Writing payloads...')
    for payload in tqdm(payloads):
        
        filepath = os.path.join(
            payload_dst,
            payload.get_filename()
        )

        payload.to_json(filepath)
    
    print(f'Payloads available at {payload_dst}')

        


def get_parser_generate_payloads(parent=None):
    """A parser to generate payloads.

    Args:
        parent (object, optional): Parent parser object. Defaults to None.
    """
    parser = argparse.ArgumentParser() if parent is None else parent.add_parser('drs_gen_payloads')

    parser.description = 'Generate a series of payload files.'
    parser.add_argument('payload_dst', type=str, help='Where to write payload files.')
    parser.add_argument('input_files', type=str, help='Globbable path to input files, use quotes.')
    parser.add_argument('output_dir', type=str, help='Output directory (DRS written from here).')
    parser.add_argument('start_year', type=int, help='Start year.')
    parser.add_argument('end_year', type=int, help='End year.')
    parser.add_argument('project', type=str, help='Project key from projects.json.')
    parser.add_argument('model', type=str, help='Model key from models.json.')
    parser.add_argument('domain', type=str, help='Domain key from domains.json.')

    parser.add_argument('--variables', type=split_args, help='Comma-separated list of variables to process.')
    parser.add_argument('--schema', type=str, help='Schema to read variables from in lieu of variables.')
    parser.add_argument('--output_frequencies', type=split_args, help='Comma-separated list of output frequencies. Defaults to "1H,6H,1D,1M"', default='1H,6H,1D,1M')

    parser.add_argument('-e', '--extra', type=str, nargs=argparse.ZERO_OR_MORE, help='Extra metadata to add, "key,value".')
    parser.set_defaults(func=generate_payloads)

    return parser


def rerun_failures(input_dir):
    """Method to rerun payloads based on the .failed output files.
    
    Args:
        input_dir (str) : Path to the input directory containing both the failed files and the original payloads.
    """

    # Find all of the failed files in the provided directory
    failed_filepaths = au.auto_glob(f'{input_dir}/*failed')

    for failed_filepath in failed_filepaths:

        # Open the failed filepath, read out the variables
        raw = open(failed_filepath).read().splitlines()
        failed_variables = [_raw.split(',')[0] for _raw in raw]

        # Filter out duplicates
        failed_variables = list(set(failed_variables))

        # Open the payload
        payload_filename = os.path.basename(failed_filepath).split('_')[0]
        payload_filepath = os.path.join(
            input_dir,
            payload_filename
        )

        # Open them
        payload = adp.Payload.from_json(payload_filepath)

        # Replace the variables listed with that of those included in the failed file
        payload.variables = failed_variables
        
        # Save them to a rerun directory
        output_dir = os.path.join(input_dir, 'rerun')
        os.makedirs(output_dir, exist_ok=True)

        output_filepath = os.path.join(
            output_dir,
            payload_filename
        )

        print(output_filepath)
        payload.to_json(output_filepath)


def get_parser_rerun_failures(parent=None):
    """Get a parser for rerunning payloads.

    Args:
        parent (object, optional): Parent parser. Defaults to None.
    """
    parser = argparse.ArgumentParser() if parent is None else parent.add_parser('drs_rerun_failures')
    parser.description = 'Generate rerun payloads from the .failed files in the input directory'
    parser.add_argument('input_dir', type=str, help='Path to .failed files and their payloads.')
    parser.set_defaults(func=rerun_failures)
    return parser


def get_parser_generate_user_config(parent=None):
    """Get a parser for generating a set of user config files from the installation directory.

    Args:
        parent (object, optional): Parent parser. Defaults to None.
    """
    parser = argparse.ArgumentParser() if parent is None else parent.add_parser('drs_gen_user_config')
    parser.description = 'Copy installation configuration to the user space (backing up anything already there).'
    parser.set_defaults(func=adu.generate_user_config)
    return parser