"""Test utility functions."""
import axiom.drs.utilities as adu

def test_is_error_recoverable():
    """Test is_error_recoverable."""
    
    rex = Exception('lock_acquire')
    urex = Exception('unrecoverable')

    recoverable_errors = [
        'lock_acquire'
    ]

    assert adu.is_error_recoverable(rex, recoverable_errors) == True
    assert adu.is_error_recoverable(urex, recoverable_errors) == False


def test_assemble_qsub_vars():
    """Test assemble_qsub_vars."""
    
    result = adu.assemble_qsub_vars(myvar1='myval1', myvar2='myval2')
    assert result == 'myvar1=myval1,myvar2=myval2'


def test_assemble_qsub_command():
    """"Test assemble_qsub_command."""
    
    directives = [
        '-l walltime=%(walltime)s',
        '-q %(queue)s',
        '-N %(jobname)s',
        '-v %(qsub_vars)s'
    ]

    context = dict(
        walltime='00:30:00',
        queue='normal',
        jobname='myjob',
        qsub_vars=adu.assemble_qsub_vars(myvar1='myval1', myvar2='myval2')
    )

    result = adu.assemble_qsub_command('test.sh', directives, **context)
    expected = 'qsub -l walltime=00:30:00 -q normal -N myjob -v myvar1=myval1,myvar2=myval2 test.sh'
    
    assert result == expected