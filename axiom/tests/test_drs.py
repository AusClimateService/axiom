"""Tests for the DRS subsystem."""
import re
import axiom.drs.utilities as adu


def test_parse_domain():
    """Test parsing a domain."""
    directive = "Antarctica,0.5,-90.0,-50.0,-180.0,180.0"

    expected = dict(
        name='Antarctica',
        dx=0.5,
        lat_min=-90.0, lat_max=-50.0,
        lon_min=-180.0, lon_max=180.0
    )

    result = adu.parse_domain(directive)
    assert result == expected


def test_get_uninterpolated_placeholders():
    """Test uninterpolated value check."""
    
    expect_something = 'this is %(a)s test of %(b)s'
    result = adu.get_uninterpolated_placeholders(expect_something)
    assert result == ['a', 'b']

    expect_nothing = 'this is a test of b'
    result = adu.get_uninterpolated_placeholders(expect_nothing)
    assert len(result) == 0