"""Command-line methods for the DRS subsystem."""
import argparse
import axiom.utilities as au


def get_parser(config=None, parent=None):
    """Parse arguments for command line utiltities.

    Args:
        config (dict) : Configuration dictionary.
        parent (obj) : Parent parser object (for integration into the main axiom CLI)

    Returns:
        argparse.Namespace : Arguments object.
    """

    if config is None:
        config = au.load_package_data('data/drs.json')

    # Build a parser
    if parent is None:
        parser = argparse.ArgumentParser()
    # ...or add one to the top-level CLI
    else:
        parser = parent.add_parser('drs')

    parser.description = "DRS utility"

    # Paths
    parser.add_argument('input_files', type=str, help='Input filepaths', nargs=argparse.ONE_OR_MORE)
    parser.add_argument('output_directory', type=str, help='Output base directory (DRS structure built from here)')
    parser.add_argument('-o', '--overwrite', default=False, help='Overwrite existing output', action='store_true')

    # Temporal
    parser.add_argument('-s', '--start_year', required=True, type=int, help='Start year')
    parser.add_argument('-e', '--end_year', required=True, type=int, help='End year')

    # Resolution and output frequency
    parser.add_argument('-r', '--input_resolution', type=float, help='Input resolution in km, leave blank to auto-detect from path.')
    
    parser.add_argument(
        '-f', '--output_frequency', required=True, type=str, metavar='output_frequency',
        help='Output frequency, Examples include "12min", "1M" (1 month) etc. see https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases.'
    )

    # Metadata
    parser.add_argument('-p', '--project', required=True, type=str, choices=config['projects'].keys())
    parser.add_argument('-m', '--model', required=True, type=str, choices=config['models'].keys())

    # Domains, we can process multiple at once
    parser.add_argument(
        '-d', '--domains',
        required=True, type=str,
        choices=config['domains'].keys(),
        nargs='*', metavar='domain',
        help='Domains to process, space-separated.'
    )

    # Override the variables defined in the drs.json file
    parser.add_argument(
        '-v', '--variables',
        type=str,
        nargs='*',
        metavar='variables',
        help='Variables to process, omit to use those defined in config.'
    )

    # Switch on CORDEX options
    parser.add_argument(
        '--cordex',
        help='Process for CORDEX',
        action='store_true',
        default=False
    )

    return parser


def get_parser_consume(config=None, parent=None):
    """A parser that can consume JSON payload files.

    Args:
        config ([type], optional): [description]. Defaults to None.
        parent ([type], optional): [description]. Defaults to None.
    """
    if parent is None:
        parser = argparse.ArgumentParser()
    else:
        parser = parent.add_parser('drs_consume')

    # Input filepaths
    parser.add_argument('input_filepaths', type=str,
                        help='Input json filepaths.', nargs=argparse.ONE_OR_MORE)
    return parser
