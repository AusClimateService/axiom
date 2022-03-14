"""Custom exception classes."""

class ResolutionDetectionException(Exception):
    """Raised when tere are issues detecting resolution information."""
    pass

class MalformedDRSJSONPayloadException(Exception):
    """Raised when a DRS JSON payload can't be processed."""
    pass