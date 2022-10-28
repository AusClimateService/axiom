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