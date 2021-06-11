"""Reporting functions."""
from tabulate import tabulate
from axiom import __version__


def _is_missing(e):
    """Check if error was for a missing attribute.

    Args:
        e (cerberus.errors.ValidationError): Error object.

    Returns:
        bool: True/False
    """
    return isinstance(e.constraint, bool) and e.constraint == True and e.value == None


def _is_incorrect_type(e):
    """Check if error was for an incorrect attribute data type.

    Args:
        e (cerberus.errors.ValidationError): Error object.

    Returns:
        bool: True/False
    """
    return e.schema_path[-1] == 'type'


def _is_incorrect_value(e):
    """Check if error was for an incorrect attribute value.

    Args:
        e (cerberus.errors.ValidationError): Error object.

    Returns:
        bool: True/False
    """
    return e.schema_path[-1] == 'allowed'


def generate_report(validator, input_filepath, report_filepath):
    """Generate a report for a validator.

    Args:
        validator (axiom.validation.Validator): Validator object.
        input_filepath (str) : Path to  the file that was validated.
        report_filepath (str): Path to which to write the report.
    """

    # Create the headings and lines array
    table_heading = ['Variable', 'Attribute', 'Error']
    table_lines = list()

    # Global errors
    for e in validator.errors['_global']:

        variable = '_global'
        attribute = e.document_path[0]

        # Attribute missing
        if _is_missing(e):
            error = 'Attribute is missing'

        # Incorrect type
        elif _is_incorrect_type(e):
            expected_type = e.constraint
            error = f'Incorrect type, should be {expected_type}'

        else:
            error = 'Unknown error'

        table_lines.append([variable, attribute, error])


    # Errors per variable
    for key, errors in validator.errors['variables'].items():

        variable = key

        for e in errors:

            attribute = e.document_path[0]

            # Missing
            if _is_missing(e):
                error = "Attribute is missing."

            # Incorrect type
            elif _is_incorrect_type(e):
                expected_type = e.constraint
                error = f'Incorrect type, should be {expected_type}'

            # Incorrect value
            elif _is_incorrect_value(e):
                allowed_values = ','.join(e.constraint)
                error = f'Incorrect value, must be one of {allowed_values}'

            else:
                error = 'Unknown error'

            line = [variable, attribute, error]
            table_lines.append(line)


    # Tabulate reults
    table = tabulate(table_lines, table_heading, tablefmt='grid')

    # Determine the status
    status = 'PASSED' if validator.is_valid else 'FAILED'

    # Generate the heading.
    lines = list()
    lines.append(f'Axiom Validator {__version__}')
    lines.append(f'Report generated: {validator.date_validated}')
    lines.append(f'schema_filepath: {validator.schema_filepath}')
    lines.append(f'input_filepath: {input_filepath}')
    lines.append(f'Status: {status}\n')

    # Put it all together and dump to file
    content = '\n'.join(lines)
    content = content + table + '\n'
    open(report_filepath, 'w').write(content)
