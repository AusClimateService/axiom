"""Test the configuration object."""
from axiom.config import Config


def test_boolean_fallback():
    """Test that a missing config key returns False."""
    c = Config()
    assert c.missing_key == False