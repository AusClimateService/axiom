class Validator:

    def __init__(self):
        self.errors = list()
        self.is_valid = False

    def validate(self, xml, xsd):
        """Validate xml against xsd.

        Args:
            xml (lxml.etree.ElementTree) : XML object.
            xsd (xmlschema.XMLSchema) : Schema object.

        Returns:
            bool : True if valid, False otherwise.
        """
        pass

    def generate_report(self, output_filepath):
        """Write report to output_filepath.

        Args:
            output_filepath (str) : Path.
        """
        pass
