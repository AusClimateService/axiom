"""Quality-Assurance module."""
import glob
import pandas as pd
import numpy as np
import axiom.utilities as au
from tqdm import tqdm
import os


def check_timeseries_variable(search_template, variable, start_year, end_year):
    """Check the timeseries for a given variable.

    Args:
        search_template (str): Globbable search path with %(variable)s and %(year)s placeholders.
        variable (str or list): Variable name(s).
        start_year (int): Start year.
        end_year (int): End year.
    
    Returns:
        pandas.DataFrame: DataFrame of results.
    """

    rows = list()

    for _variable in tqdm(au.pluralise(variable)):

        for year in range(start_year, end_year + 1):

            context = dict(variable=_variable, year=year)

            filepath_search = search_template % context
            # filepath_search = au.interpolate_template(search_template, **context)
            filepaths = sorted(glob.glob(filepath_search))

            # row = dict(filepath_search=filepath_search)
            row = dict(variable=_variable, year=year, filepath_search=filepath_search)

            # Success: file is present
            if len(filepaths) == 1:

                # Get the filesize in mb
                num_bytes = os.path.getsize(filepaths[0])
                filesize_mb = num_bytes / (1024 * 1024)
                _row = dict(status='SUCCESS', comment='File is present.', size=filesize_mb)

            # Error: no file
            elif len(filepaths) == 0:
                _row = dict(status='ERROR', comment='File is missing.', size=np.nan)

            # Error: multiple files detected
            else:
                _row = dict(status='ERROR', comment='Multiple files detected.', size=np.nan)

            row.update(_row)
            rows.append(row)
    
    df = pd.DataFrame.from_records(rows)
    return df