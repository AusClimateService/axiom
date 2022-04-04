"""Indices module."""
from multiprocessing.sharedctypes import Value
import xclim as xc
import xclim.indicators.atmos as xia
import xclim.indices.generic as xig
import xclim.indicators.icclim as xii
import xarray as xr
from distributed import Client, progress
# from dask.diagnostics import ProgressBar

# Climpact to xclim mappings
# 'FD', 'tasmin < 0.0', 'YS'

class Index:

    def __init__(self, name, variable, operation, threshold, frequency, **kwargs):
        """Index object.

        Args:
            name (str): Name of the index.
            variable (str): Name of the variable used to calculate the index.
            operation (str): Logical operation.
            threshold (float): Threshold for computation.
            frequency (str): Frequency.
            **kwargs: Additional key/value pairs to add as metadata to the result.
        """
        self.name = name
        self.variable = variable
        self.operation = operation
        self.threshold = threshold
        self.frequency = frequency
        self.metadata = kwargs

        # Add Metadata
        self.metadata['description'] = f'Number of days where {variable} {operation} {threshold}'
        self.metadata['variable'] = variable
        self.metadata['threshold'] = threshold
        self.metadata['operation'] = operation
        self.metadata['frequency'] = frequency

        # Version information
        self.metadata['axiom_version'] = '0.1.1'
        self.metadata['xclim_version'] = xc.__version__

        self.func = xig.threshold_count

    
    def calculate(self, ds):
        """Calculate the index.

        Args:
            ds (xarray.Dataset): Data.

        Returns:
            xarray.DataArray: Result.
        """
        da = ds[self.variable]
        result = self.func(da, self.operation, self.threshold, self.frequency)
        result.name = self.name

        # Add some basic metadata.
        result.attrs.update(self.metadata)
        return result


    def from_directive(directive):
        """Convert a string directive into an index.

        Example Directive: frost_days tasmin < 0 YS

        Args:
            directive (str): Directive string.
        
        Returns:
            axiom.indices.Index : Index object.
        """

        segments = directive.split(' ')

        assert len(segments) == 5

        name, variable, operation, threshold, frequency = segments
        threshold = float(threshold)

        return Index(name, variable, operation, threshold, frequency)

# 'frost_days = tasmin < 0.0'

mappings = {
    'FD': dict(func='atmos.tn_days_below', thresh=0.0, freq='YS'),
    'TNlt2': dict(func='atmos.tn_days_below', thresh=2.0, freq='YS'),
    'TNltm2': dict(func='atmos.tn_days_below', thresh=-2.0, freq='YS'),
    'TNltm20': dict(func='atmos.tn_days_below', thresh=-20.0, freq='YS'),
    'SU': dict(func='atmos.tx_days_above', thresh=25.0, freq='YS'),
    'ID': dict(func='atmos.tx_days_below', thresh=0.0, freq='YS'),
    'TR': dict(func='atmos.tn_days_above', thresh=20, freq='YS'),
    'GSL': None,
    'TXx': dict(func='cf.txx', freq='MS'),
    'TNx': dict(func='cf.tnx', freq='MS'),
    'TXn': dict(func='cf.txn', freq='MS'),
    'TNn': dict(func='cf.tnn', freq='MS'),
    'TMm': dict(func='cf.tmm', freq='DS'),
    'TXm': dict(func='cf.txm', freq='DS'),
    'TNm': dict(func='cf.tnm', freq='DS'),
}   


def calculate(ds, index):

    # Get the index function
    func = getattr(xii, index)

    # Calculate the index
    return func(ds)

if __name__ == '__main__':

    # Get the directories
    base_dir = '/g/data/xv83/bxn599/CaRSA/climpact_agcd/data'
    input_file = 'agcd_v1_tmin_mean_r005_daily_1980-2019.nc'
    input_filepath = f'{base_dir}/{input_file}'
    output_filepath = '/scratch/xv83/bjs581/fd.nc'

    # index = Index.from_directive('frost_days tasmin < 0.0 YS')

    # print(index)

    # print('Starting client')
    # client = Client()
    # print(client)

    print('Opening data')
    ds = xr.open_dataset(input_filepath, chunks=dict(time=1))

    result = calculate(ds, 'FD')

    print(FD)
    
    # print('Calculating')
    # result = fd(ds.tmin)
    # tasmin = ds.tmin
    # tasmin.attrs['units'] = 'degC'
    # result = xia.frost_days(tasmin)
    # result = index.calculate(ds.rename(tmin='tasmin'))

    # print(result)

    # print(f'Writing to {output_filepath}')
    # write = result.to_netcdf(output_filepath)

    # # progress(write.compute())
