"""Custom exception classes."""

class ResolutionDetectionException(Exception):
    """Raised when tere are issues detecting resolution information."""
    pass

class MalformedDRSJSONPayloadException(Exception):
    """Raised when a DRS JSON payload can't be processed."""
    pass

class NoFilesToProcessException(Exception):
    """Raised when there are no files to process for a DRS payload"""
    pass

class DRSContextInterpolationException(Exception):
    """Raised when there are remaining placeholders in a template after DRS context interpolation.
    
    Args:
        placeholders (list) : List of placeholders that have yet to be interpolated.
    """
    def __init__(self, placeholders):
        msg = 'The following placeholders have been unsuccessfully interpolated:\n'
        msg += '\n'.join(placeholders)
        super().__init__(msg)
