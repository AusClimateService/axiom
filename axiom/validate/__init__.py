"""Validation module."""

def nonempty_str(value):
    """Check if a value is a non-empty string.

    Args:
        value (str): Value to test.

    Returns:
        : [description]
    """
    if value.strip() != '':
        return value.strip()
    
    raise ValueError(f'Value {value} is an empty string (once stripped)')