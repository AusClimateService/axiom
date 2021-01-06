"""Metadata templates."""
import os
import sys
from datetime import datetime
import axiom as ax

# Default template
DEFAULT = dict(
    author=os.getenv('USER'),
    pwd=os.getcwd(),
    command=' '.join(sys.argv),
    created=datetime.utcnow(),
    axiom_version=ax.__version__
)