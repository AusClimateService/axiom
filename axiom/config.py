"""Configuration."""
import os
import json
import pkgutil
import pathlib
import axiom.utilities as au


class Config(dict):
    

    def __getattr__(self, key, default=None):
        """Allows attribute access for top-level keys.

        Args:
            key (str): Key.

        Returns:
            Any: Value.
        """
        return self.__getitem__(key)


    def __setattr__(self, key, value):
        """Allows attribute access for top-level keys.

        Args:
            key (str): Key.
            value (Any): Value to assign.
        """
        super().__setitem__(key, value)

    def __getitem__(self, key):
        """Boolean fallback method, returning False if key does not exist.

        Use for adding new conditional functionality.

        Args:
            key (hashable): Key

        Returns:
            object : Value in config, or False
        """
        
        try:
            value = super().__getitem__(key)
            
        except KeyError:
            value = False

        return value

    def get(self, key, default=None):
        
        if key in self.keys():
            return self[key]

        if default:
            return default

        raise KeyError(f'Config key {key} does not exist.')
        

    def load(self, config_name, defaults_only=False):
        """Load the configuration in a cascading fashion, defaults first, then overlay with user.

        Args:
            config_name (str): Configuration name, without file extension.
            defaults_only (bool, optional): Load only the defaults. Defaults to False.
        """

        default_filepath = os.path.join(au.get_installed_data_root(), f'{config_name}.json')
        user_filepath = os.path.join(au.get_user_data_root(), f'{config_name}.json')
        
        # Load any installed defaults, if they exists
        if os.path.isfile(default_filepath):
            defaults = json.load(open(default_filepath, 'r'))
        else:
            defaults = dict()

        if defaults_only:
            self.update(defaults)
            return

        # Load the user configuration over the top
        if os.path.isfile(user_filepath):
            user = json.load(open(user_filepath, 'r'))
            defaults.update(user)

        self.update(defaults)


def load_config(config_name, defaults_only=False):
    """Shorthand to load a configuration object.

    Args:
        config_name (str): Name of the config file.
        defaults_only (bool, optional): Load only the defaults. Defaults to False.

    Returns:
        axiom.Config: Configuration object.
    """
    config = Config()
    config.load(config_name, defaults_only=defaults_only)
    return config