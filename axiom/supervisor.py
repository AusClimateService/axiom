"""Supervisor class for handling long-running jobs."""
import signal

class Supervisor:

    """Supervisor context manager class. Used to supervise a section of code and ensure it completes in a given time.

    Usage:
        >>> with Supervisor(seconds=10, error_msg='Code took too long.'):
        >>>     # ... code that should complete in less than 10 seconds.

    Args:
        seconds (int): Number of seconds to allow for the contained code to run.
        error_msg (str): Error message to raise with the TimeoutError.
    """
 
    def __init__(self, seconds, error_msg):
        self.seconds = seconds
        self.error_msg = error_msg
    

    def _timeout(self, signum, frame):
        """Raise a TimeoutError.

        Args:
            signum : Not used, added for compatibility.
            frame : Not used, added for compatibility.

        Raises:
            TimeoutError: When the number of seconds expires.
        """
        raise TimeoutError(self.error_msg)


    def __enter__(self):
        """Enter the context manager."""
        # Start a timer for how long the code may be in this manager.
        signal.signal(signal.SIGALRM, self._timeout)
        signal.alarm(self.seconds)
    

    def __exit__(self, type, value, traceback):
        """Exit the context manager."""
        # Cancel any set timers.
        signal.alarm(0)