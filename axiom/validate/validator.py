"""Validation object."""
import axiom as ax
import axiom.utilities as au
from axiom.exceptions import ValidationException

class Validator:

    def __init__(self, specification):
        """Class to validate a data file against a validation standard."""
        self.specification = specification

    def validate(self, filepath_or_obj):
        """Validate the data at filepath_or_obj against a standard.

        Args:
            filepath_or_obj (str, xarray.Dataset): Path to the file, or the loaded dataset.
        
        Returns:
            bool : True if file contains valid metadata for the standard, False otherwise.
        """
        logger = au.get_logger(__name__)

        logger.info(f'Axiom Validator v{ax.__version__}')
        
        # Load the filepath
        if isinstance(filepath_or_obj, str):
            filepath_or_obj = ax.load_data(filepath_or_obj)    

        # For convenience
        ds = filepath_or_obj

        filepath = ds.encoding['source']
        logger.info(f'Validating {filepath}')
        
        exceptions = list()

        # Check the global attributes
        logger.debug('Validating global attributes.')
        for attr, spec in self.specification.required['_global'].items():

            logger.debug(f'Attribute: {attr}')

            try:

                attr_value = ds.attrs[attr]

                for func in spec:
                    logger.debug(f'Checking {func}')
                    attr_value = func(attr_value)
                
            except Exception as ex:
                logger.error(ex)
                exceptions.append(ex)

        # Check if there were any exceptions
        if len(exceptions) > 0:
            raise ValidationException('FAIL')
        
        logger.info(f'SUCCESS! File {filepath} meets {self.specification} specification.')