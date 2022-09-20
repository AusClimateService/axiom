"""Class to represent a JSON payload."""
import json


class Payload:

    """Class to represent a Payload object.

    Args:
        input_files (str): Globbable path to input files.
        output_directory (str): Output directory.
        start_year (int) : Start year.
        end_year (int): End year.
        output_frequency (str): Output frequency.
        project (str): Project key from projects.json.
        model (str): Model key from models.json.
        domain (str): Domain key from domains.json.
        variables (list, optional): List of variables to process. Defaults to [].
        **kwargs : Additional key/value pairs added as a metadata.
    
    Attributes:
        input_files (str): Globbable path to input files.
        output_directory (str): Destination output directory (DRS built from here).
        project (str): Project key from projects.json.
        model (str): Model key from models.json.
        domain (str): Domain key from domains.json.
        start_year (int): Start year.
        end_year (int): End year.
        variables (list): List of variable names to process.
        output_frequency (str): Output frequency/resolution of data.
        extra (dict): Additional metadata key/value pairs.
    """

    def __init__(self, input_files, output_directory, start_year, end_year, output_frequency, project, model, domain, variables=[], **kwargs):
        
        self.input_files = input_files
        self.output_directory = output_directory
        
        self.project = project
        self.model = model
        self.domain = domain

        self.start_year = start_year
        self.end_year = end_year
        
        self.variables = variables
        self.output_frequency = output_frequency
        self.extra = kwargs
    

    def get_filename(self):
        """Generate a filename for this payload.

        Returns:
            str: Filename
        
        Examples:
            >>> filename = payload.get_filename()
        """
        return f'payload.{self.start_year}.{self.output_frequency}.json'
    

    def to_dict(self):
        """Convert the Payload object to a dictionary.

        Returns:
            dict: Dictionary representation of the Payload.
        
        Examples:
            >>> payload.to_dict()
        """
        _dict = self.__dict__.copy()
        extra = _dict.pop('extra')
        _dict.update(extra)
        return _dict
    

    def from_dict(d):
        """Create a Payload from a dictionary.

        Args:
            d (dict): Dictionary.

        Returns:
            axiom.drs.Payload: Payload object.
        
        Examples:
            >>> payload = Payload.from_dict(d)
        """
        return Payload(**d)
    

    def to_json(self, filepath):
        """Serialize the Payload to the filepath (save as JSON).

        Args:
            filepath (str): Filepath.
        
        Examples:
            >>> payload.to_json('payload.json')
        """
        _dict = self.to_dict()
        with open(filepath, 'w') as f:
            f.write(json.dumps(_dict, indent=4))


    def from_json(filepath):
        """Instantiate a payload from a json filepath.

        Args:
            filepath (str): Path.

        Returns:
            axiom.drs.Payload: Payload.
        
        Examples:
            >>> payload = Payload.from_json('payload.json')
        """
        d = json.load(open(filepath, 'r'))
        return Payload.from_dict(d)