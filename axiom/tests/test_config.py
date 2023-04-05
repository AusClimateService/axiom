"""Test the configuration object."""
from axiom.config import Config, load_config


def test_boolean_fallback():
    """Test that a missing config key returns False."""
    c = Config()
    assert c.missing_key == False


def test_keys_in_installed_drs_json():
    """A test to ensure that new local dev keys have been added to the data/drs.json file."""

    user_config = load_config('drs', defaults_only=False)
    default_config = load_config('drs', defaults_only=True)

    missing_keys = list()

    for key in user_config.keys():
        if key not in default_config.keys():
            missing_keys.append(key)
    
    if len(missing_keys) > 0:
        raise AssertionError('The following keys are missing from the installed defaults: ' + ', '.join(missing_keys))