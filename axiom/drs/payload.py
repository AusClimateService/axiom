"""Class to represent a JSON payload."""
import enum
import json
import axiom.schemas as axs
import axiom.utilities as au


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

    def __init__(self, input_files, output_directory, start_year, end_year, output_frequency, project, model, domain, variables=[], batch=1, **kwargs):
        
        self.input_files = input_files
        self.output_directory = output_directory
        
        self.project = project
        self.model = model
        self.domain = domain

        self.start_year = start_year
        self.end_year = end_year
        
        self.variables = variables
        self.output_frequency = output_frequency

        self.batch = batch

        self.extra = kwargs
    

    def get_filename(self):
        """Generate a filename for this payload.

        Returns:
            str: Filename
        
        Examples:
            >>> filename = payload.get_filename()
        """
        bbb = str(self.batch).zfill(3)
        return f'payload.{self.start_year}.{self.output_frequency}.{bbb}.json'
    

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


def generate_payloads(input_files, output_directory, start_year, end_year, project, model, domain, variables=None, schema=None, output_frequencies=['1H', '6H', '1D', '1M'], num_batches=1, **extra):
    """Generate payload files.

    Args:
        input_files (str): Globbable path to input files.
        output_directory (str): Destination filepath, DRS will be built from here.
        start_year (int): Start year.
        end_year (int): End year.
        project (str): Project key from projects.json.
        model (str): Model key from models.json.
        domain (str): Domain key from domains.json.
        variables (list(str), optional): List of variables to project. Defaults to None (read all from schema).
        schema (str): Schema name or filepath.
        output_frequencies (list(str), optional): List of output frequencies. Defaults to ['1H', '6H', '1D', '1M'].
        num_batches (int, optional): Number of batches to split processing into. Defaults to 1.
        **extra : Key/value pairs added as additional metadata.
    
    Returns:
        list : A list of payload objects.
    """

    payloads = list()

    # Load the variables from the schema if not provided
    if variables is None:
        schema = axs.load_schema(schema)
        variables = list(schema['variables'].keys())    
    
    # Batch if required (could be a single batch)
    batches = au.batch_split(variables, num_batches)

    for year in range(start_year, end_year+1):

        for output_frequency in output_frequencies:

            for batch_ix, batch in enumerate(batches):
        
                # Generate a payload
                payload = Payload(
                    input_files=input_files,
                    output_directory=output_directory,
                    project=project,
                    model=model,
                    domain=domain,
                    start_year=year,
                    end_year=year,
                    variables=batch,
                    output_frequency=output_frequency,
                    batch=batch_ix,
                    **extra
                )

                payloads.append(payload)
    
    return payloads