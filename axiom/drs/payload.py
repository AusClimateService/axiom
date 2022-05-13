"""Class to represent a JSON payload."""
import json


class Payload:

    """Class to represent a JSON payload for DRS processing.
    
    Args:
        **kwargs : key/value pairs of payload information.
    """

    def __init__(self, **kwargs):
        self._payload = kwargs


    def from_dict(d):
        """Instantiate a payload from a dictionary.

        Args:
            d (dict): Dictionary.

        Returns:
            axiom.drs.Payload: Payload object
        """
        return Payload(**d)
    

    def to_dict(self):
        """Dump the payload to a dictionary.

        Returns:
            dict: Payload dictionary.
        """
        return self._payload


    def to_json(self, filepath):
        """Dump the payload to a json filepath.

        Args:   
            filepath (str): Output filepath.
        """
        with open(filepath, 'w') as f:
            f.write(json.dumps(self._payload, indent=4))


    def from_json(filepath):
        """Instantiate a payload from a json filepath.

        Args:
            filepath (str): Path.

        Returns:
            axiom.drs.Payload: Payload.
        """
        d = json.load(open(filepath, 'r'))
        return Payload.from_dict(d)


    def __setattr__(self, key, value):
        """Set an attribute on the payload.

        Args:
            key (str): Key.
            value (obj): Value.
        """
        self._payload[key] = value
    

    def __getattr__(self, key):
        """Get attribute from payload.

        Args:
            key (str): Key.

        Returns:
            obj: Value
        """
        return self._payload[key]