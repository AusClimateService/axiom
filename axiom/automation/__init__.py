"""Automation module."""
import os
import sys
import json
import time
import datetime
from axiom.config import load_config
import axiom.utilities as au
from axiom.drs.payload import Payload
from axiom.schemas import load_schema
import axiom.drs.cli as adc


def get_instance_dir(instance):
    """Get the instance directory.

    Args:
        instance (dict): Processing instance.

    Returns:
        str: Instance directory.
    """
    return os.path.join(
        instance['work_dir'],
        instance['experiment'],
        instance['frequency']
    )


def generate_payloads(instance):
    """Generate the payloads for the processing instance.

    Args:
        instance (dict): Processing instance.
        work_dir (str): Working directory.
        experiment_base_dir (str): Base directory for the experiment.
    """
    logger = au.get_logger(__name__)

    experiment = instance['experiment']
    frequency = instance['frequency']
    logger.info(f'Generating payloads for {experiment} {frequency}')

    # Load the template payload
    payload_template = Payload.from_json(instance['payload_template'])

    # Load the schema and extract variables
    schema = load_schema(instance['schema'])
    variables = list(schema['variables'].keys())
    
    # Cut into batches of variables
    batches = au.batch_split(variables, instance['batches'])

    # Get the instance directory
    instance_dir = get_instance_dir(instance)
    payload_dst = os.path.join(
        instance_dir,
        'payloads'
    )

    os.makedirs(payload_dst, exist_ok=True)

    for year in range(instance['start_year'], instance['end_year'] + 1):
        for batch_ix, batch in enumerate(batches):
            
            # Batch identifier
            bbb = str(batch_ix).zfill(3)

            # Assemble the payload
            payload = payload_template
            payload.start_year = year
            payload.end_year = year

            # Add the variables included in this batch
            payload.variables = list(batch)
            payload.output_frequency = instance['frequency']

            # Write the payload file, with a batch identifier
            output_filename = payload.get_filename().replace('.json', f'.{bbb}.json')
            output_filepath = os.path.join(payload_dst, output_filename)
            print(output_filepath)
            payload.to_json(output_filepath)


def get_state(instance_dir, step):
    
    state_file = os.path.join(instance_dir, 'state', f'{step}.state')

    if os.path.isfile(state_file) is False:
        return False
    
    # Get the last line of the file
    with open(state_file, 'r') as f:
        lines = f.readlines()
        last_line = lines[-1]
    
    # Remove the newline character
    last_line = last_line.replace('\n', '')

    return last_line


def update_state(instance_dir, step, message):
    state_file = os.path.join(instance_dir, 'state', f'{step}.state')

    os.makedirs(os.path.dirname(state_file), exist_ok=True)

    with open(state_file, 'a') as f:
        f.write(f'{message}\n')


def check_failed(instance):
    """Check if any payloads have failed to process."""
    
    instance_dir = get_instance_dir(instance)
    
    payload_dst = os.path.join(
        instance_dir,
        'payloads'
    )

    failed_filepaths = au.auto_glob(os.path.join(payload_dst, '*.failed'), recursive=False)

    if len(failed_filepaths) > 0:
        return False


def check_consumed(instance):
    """Check that the number of payloads consumed matches the number of payloads generated.

    Args:
        instance (dict): Instance

    Returns:
        bool: True if all payloads have been consumed, False otherwise.
    """
    instance_dir = get_instance_dir(instance)
    num_payloads = len(au.auto_glob(os.path.join(instance_dir, 'payloads', '*.json')))
    num_consumed = len(au.auto_glob(os.path.join(instance_dir, 'payloads', '*.consumed')))
    return num_payloads == num_consumed


def submit_payloads(instance):

    instance_dir = get_instance_dir(instance)
    log_dir = os.path.join(instance_dir, 'logs')

    launch_kw = instance['drs_launch_kwargs']

    launch_kw['path'] = os.path.join(instance_dir, 'payloads', '*.json')
    launch_kw['log_dir'] = log_dir

    # Submit the job
    adc.drs_launch(
        **launch_kw
        # os.path.join(instance_dir, 'payloads', '*.json'),
        # instance['jobscript'],
        # log_dir,
        # dry_run=True
    )


def process(instance):


    logger = au.get_logger(__name__)
    
    # Check if payloads have been generated
    instance_dir = get_instance_dir(instance)

    # Generate payloads
    if get_state(instance_dir, 'generate_payloads') != 'success':
        generate_payloads(instance)
        update_state(instance_dir, 'generate_payloads', 'success')
    logger.info('Payloads generated')
    
    # Submit payloads
    if get_state(instance_dir, 'submit_payloads') != 'success':
        logger.info('Submitting payloads')
        submit_payloads(instance)
        update_state(instance_dir, 'submit_payloads', 'success')

    logger.info('Payloads submitted')

    # Get the number of jobs running
    logger.info('Checking job status')
    num_payloads = len(au.auto_glob(os.path.join(instance_dir, 'payloads', '*.json')))
    num_consumed = len(au.auto_glob(os.path.join(instance_dir, 'payloads', '*.consumed')))
    num_failed = len(au.auto_glob(os.path.join(instance_dir, 'payloads', '*.failed')))

    # Check if anything has failed
    if num_failed > 0:
        logger.info('Failures detected.')
        raise InstanceFailedError(f'{num_failed} payloads failed to process')
    
    # Check if all payloads have been consumed
    if num_payloads == num_consumed:
        logger.info('All payloads consumed')
        return True
    else:
        logger.info('Job is still running...')
        return False


def process_all():

    logger = au.get_logger(__name__)

    # Load the config
    logger.info('Loading automation configuration')
    automation_config = load_config('automation')
    instances = automation_config['instances']
    
    num_instances = len(instances)
    num_complete = 0
    num_failed = 0

    logger.info(f'Processing {num_instances} instances')
    for instance in instances:

        try:
            
            complete = process(instance)

            # If the instance is not complete, we do not want to move on to the next instance
            if not complete:
                logger.info('Instance incomplete')
                break
            else:
                logger.info('Instance complete')
                num_complete += 1
                continue

        # If there are any failures for this instance, move on to the next.
        except InstanceFailedError as e:
            num_failed += 1
            logger.error(e)
            continue
    
    # Resubmit only of there are still some instances to go
    if num_complete + num_failed < num_instances:
        
        resubmit_interval_seconds = automation_config['resubmit_interval_seconds']
        resubmit(resubmit_interval_seconds)
    
    else:

        logger.info('All instances complete')

        

class InstanceFailedError(Exception):
    pass    


def resubmit(interval_seconds):

    logger = au.get_logger(__name__)

    # If running under PBS then get the job submission information
    if 'PBS_JOBID' in os.environ:

        logger.info('Running under PBS, resubmitting...')

        # Get the job ID
        job_id = os.environ['PBS_JOBID']

        # Get the status of the job
        qstat_cmd = f'qstat -f -F json {job_id}'
        qstat = json.loads(au.shell(qstat_cmd, capture_output=True).stdout.decode('utf-8'))['Jobs'][job_id]
        submit_args = qstat['Submit_arguments']

        # Get the current time, add a delta for the next submission
        logger.info('Resubmitting for next interval...')
        now = datetime.datetime.now()
        dt = datetime.timedelta(seconds=interval_seconds)
        next_submit = (now + dt).strftime('%Y%m%d%H%M')

        qsub_cmd = f'qsub -a {next_submit} {submit_args}'
        qsub = au.shell(qsub_cmd, capture_output=True)

        if qsub.returncode == 0:
            logger.info('Resubmitted for next interval')
            new_jobid = qsub.stdout.decode('utf-8').strip()
            logger.info(f'New job ID: {new_jobid}')
            sys.exit()

    else:

        logger.info('Not running under PBS, resubmitting via Python.')
        logger.info(f'Sleeping for {interval_seconds}')
        time.sleep(interval_seconds)
        process_all()


if __name__ == '__main__':
    process_all()