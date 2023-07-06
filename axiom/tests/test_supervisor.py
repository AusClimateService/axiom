"""Code to test the supervisor class."""
import pytest
import time
from axiom.supervisor import Supervisor

def test_supervisor_fail():
    """Test the supervisor class will correctly detect a long-running job."""

    # Test that a long running job raises an exception.
    with pytest.raises(TimeoutError):
        with Supervisor(seconds=3, error_msg='Job took too long.'):
            time.sleep(5)
    
def test_supervisor_pass():
    """Test the supervisor class will not detect a jobs that completes in time."""
    # Test a job that should complete in time.
    with Supervisor(seconds=5, error_msg='Job that should work.'):
        time.sleep(2)
        assert True