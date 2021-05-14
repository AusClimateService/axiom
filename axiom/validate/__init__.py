"""Validation module."""
from tabulate import tabulate
import axiom as ax
from datetime import datetime


class ValidationError(Exception):

    """Custom exception to track error details from validation."""

    def __init__(self, key, base_error, message):
        self.key = key
        self.message = message
        super().__init__(self.message)


class Validator:

    """Validation object."""

    def __init__(self):
        self.errors = list()
        self.is_valid = False
        self.date_validated = None


    def validate(self, xml, xsd):
        """Validate xml against xsd.

        Args:
            xml (lxml.etree.ElementTree) : XML object.
            xsd (xmlschema.XMLSchema) : Schema object.

        Returns:
            bool : True if valid, False otherwise.
        """
        self.date_validated = datetime.utcnow()

        xml_dict = dict()

        # Build a dictionary for the xml
        for child in xml.iterchildren():
            xml_dict[child.tag] = dict(
                value=child.text
            )

        # Build a dictionary for the xsd
        xsd_dict = dict()
        self.errors = list()

        # Loop through the child elements to build something to test against
        for child in xsd.root_elements[0].iterchildren():

            name = child.name
            dtype = child.type.to_python
            value = child.text

            try:

                # Check it is defined
                instance = xml_dict[name]

                # Check it is the correct type
                instance_value = dtype(instance['value'])

                assert instance_value == value

            # Missing key
            except KeyError:
                self.errors.append(ValidationError(name, KeyError, message=f'Required key {name} is missing.'))

            except ValueError:
                self.errors.append(ValidationError(name, ValueError, message=f'Incorrect data type for {name}'))

            except AssertionError:
                self.errors.append(ValidationError(name, AssertionError, message=f'Value of {name} does not match schema requirements.'))

            except Exception:
                self.errors.append(ValidationError(name, Exception, message=f'Unknown error for {name}.'))

            xsd_dict[name] = dict(
                dtype=dtype,
                value=value
            )

        self.is_valid = len(self.errors) == 0
        self.status = 'PASS' if self.is_valid else 'FAIL'

        # Boolean for validation
        return self.is_valid


    def generate_report(self, output_filepath, input_filepath, schema_filepath):
        """Write report to output_filepath.

        Args:
            output_filepath (str) : Path.
        """

        table_heading = ['Key', 'Error']
        table_lines = list()

        # Generate the table
        for error in self.errors:
            table_lines.append([error.key, error.message])

        table = tabulate(table_lines, table_heading, tablefmt='grid')

        # Generate the heading.
        lines = list()
        lines.append(f'Axiom Validator {ax.__version__}')
        lines.append(f'Report generated: {self.date_validated}')
        lines.append(f'schema_filepath: {schema_filepath}')
        lines.append(f'input_filepath: {input_filepath}')
        lines.append(f'Status: {self.status}\n')

        # Put it all together and dump
        content = '\n'.join(lines)
        content = content + table
        open(output_filepath, 'w').write(content)
