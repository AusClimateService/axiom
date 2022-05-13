"""Command-line methods for the DRS subsystem."""
import os
import sys
import json
import argparse
import axiom.utilities as au
from axiom.config import load_config

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
    parser.add_argument('--batch_id', type=int, help='Batch number to process.', default=None)
    parser.add_argument('--num_batches', type=int, help='Maximum batch number.', default=None)
    return parser


def drs_launch(path, jobscript, log_dir, batches=None, dry_run=True, **kwargs):
    """Method to launch a series of qsubs for DRS processing.

    Args:
        path (str): Globbable path of payload files.
        jobscript (str): Path to the job script.
        log_dir (str): Path to which to save the log files.
        batches (int): Number of batches to split variables into (for parallel processing).
        dry_run (bool): Print out the commands rather than executing.
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

        # Skip if there is a lock file.
        if au.is_locked(payload):
            print(f'{payload} is locked.')
            continue

        # Convert the path to the jobscript to an absolute path for reproducibility
        jobscript = os.path.abspath(jobscript)

        for batch_id in _batches:

            job_name = os.path.basename(payload)
            batch_str = str(batch_id).zfill(3)
            job_name = f'{job_name}_{batch_str}'

            qsub_vars = [
                f'AXIOM_PAYLOAD={payload}',
                f'AXIOM_LOG_DIR={log_dir}'
            ]

            qsub_vars = ','.join(qsub_vars)

            cmd = f'qsub -N {job_name} -v {qsub_vars} -o {log_dir} {jobscript}'

            if 'walltime' in kwargs.keys():
                walltime = kwargs['walltime']
                cmd = f'qsub -N {job_name} -v {qsub_vars} -o {log_dir} -l walltime={walltime} {jobscript}'

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

    # Input filepaths
    parser.add_argument('path', type=str, help='Globbable path to payload files (use quotes)')
    parser.add_argument('jobscript', type=str, help='Path to the jobscript for submission.')
    parser.add_argument('log_dir', type=str, help='Directory to which to write logs.')
    parser.add_argument('-b', '--batches', type=int, help='Divide the the variables into N batches, each in its own job.', default=None)
    parser.add_argument('-d', '--dry_run', action='store_true', default=False, help='Print commands without executing.')
    parser.add_argument('--walltime', type=str, help='Override walltime in job script.')
    parser.set_defaults(func=drs_launch)

    return parser