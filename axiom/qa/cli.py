"""Command-line methods for the QA subsystem."""
import os
import argparse
import axiom_schemas as axs
import axiom.qa as axq


def qa_timeseries(path, schema, start_year, end_year, output_filepath, errors=False):
    """Scan the timeseries for missing files.
    
    Args:
        path (str): Globbable path with {variable} and {year} placeholders.
        schema (str): Schema name or path.
        start_year (int): Start year.
        end_year (int): End year.
        output_filepath (str): Path to which to write the report.
        errors (bool, Optional): Report only on errors. Defaults to False.  
    """
    
    # Load the schema
    _schema = axs.load_schema(schema)
    variables = list(_schema['variables'].keys())

    print('Checking timeseries...')
    df = axq.check_timeseries_variable(path, variables, start_year, end_year)

    # Filter only for errors.
    if errors:
        df = df[df['status'] == 'ERROR']

    # Write the report
    output_filepath = os.path.abspath(output_filepath)
    df.to_csv(output_filepath, index=False)
    
    print(f'Report available at {output_filepath}')


def get_parser(config=None, parent=None):
    """Parse arguments for the QA module.

    Args:
        config (dict, optional): Configuration dictionary. Defaults to None.
        parent (obj, optional): Parent parser object. Defaults to None.
    
    Returns:
        argparse.Namespace: Arguments object.
    """
    if parent is None:
        parser = argparse.ArgumentParser()
    else:
        parser = parent.add_parser('qa-timeseries')
    
    parser.add_argument('path', type=str, help='Globbable path with {variable} and {year} placeholders (use quotes).')
    parser.add_argument('schema', type=str, help='Schema name or path from which to load variables.')
    parser.add_argument('start_year', type=int, help='Start year.')
    parser.add_argument('end_year', type=int, help='End year.')
    parser.add_argument('output_filepath', type=str, help='Output filepath for report.')
    parser.add_argument('--errors', default=False, action='store_true', help='Output only errors.')
    parser.set_defaults(func=qa_timeseries)
    
    return parser