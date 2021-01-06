"""Validation specification."""
import axiom.validate as av
from dateutil.parser import parse as parse_date


class Specification:
    """Abstract specification class, for later enhancement."""
    pass


class V1(Specification):

    def __init__(self):
        """V1 specification object."""
        
        self.required = dict(

            # Global attributes
            _global=dict(
                author=(str, av.nonempty_str),
                pwd=(str, av.nonempty_str),
                command=(str, av.nonempty_str),
                created=(str, parse_date),
            ),

            # Variables, coordinates etc.
            lat=dict(
                attrs=dict(
                    standard_name=(str, av.nonempty_str),
                    ben_test=(str, av.nonempty_str),
                )
            )

        )


# For CLI lookups
SPECIFICATIONS = dict(
    v1=V1(),
    latest=V1()
)