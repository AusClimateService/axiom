"""Tests for the DRS subsystem."""
from axiom.drs.cli import parse_domain


def test_parse_domain():
    """Test parsing a domain."""
    directive = "Antarctica,0.5,-90.0,-50.0,-180.0,180.0"

    expected = dict(
        name='Antarctica',
        dx=0.5,
        lat_min=-90.0, lat_max=-50.0,
        lon_min=-180.0, lon_max=180.0
    )

    result = parse_domain(directive)
    assert result == expected