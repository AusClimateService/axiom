"""Configuration."""
import os
import json
import pkgutil
import pathlib


class Config(dict):
    

    def __getattr__(self, key):
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


    def load(self, config_name):
        
        # Load any installed defaults, if they exists
        try:
            
            defaults = pkgutil.get_data('axiom', f'data/{config_name}.json')
    
            if defaults is None:
                defaults = dict()
            else:
                defaults = json.loads(defaults.decode('utf-8'))
        
        except FileNotFoundError:
            
            defaults = dict()

        # Load the user configuration over the top
        user_filepath = os.path.join(pathlib.Path.home(), f'.axiom/{config_name}.json')

        if os.path.isfile(user_filepath):
            user = json.load(open(user_filepath, 'r'))
            defaults.update(user)

        # Update the object dictionary
        self.update(defaults)


def load_config(config_name):
    """Shorthand to load a configuration object.

    Args:
        config_name (str): Name of the config file.

    Returns:
        axiom.Config: Configuration object.
    """
    config = Config()
    config.load(config_name)
    return config
