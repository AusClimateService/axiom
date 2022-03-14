class Domain:

    """Domain specification class.

    Args:
        name (str): Name of the domain.
        dx (float): Delta-x.
        lat_min (float): Minimum latitude.
        lat_max (float): Maximum latitude.
        lon_min (float): Minimum longitude.
        lon_max (float): Maximum longitude.
    """

    def __init__(self, name, dx, lat_min, lat_max, lon_min, lon_max):
        self.name = name
        self.dx = dx
        self.lat_min = lat_min
        self.lat_max = lat_max
        self.lon_min = lon_min
        self.lon_max = lon_max
    

    def from_dict(domain_dict):
        """Convert from a dictionary definition to a Domain class object.
        
        Args:
            domain_dict (dict) : Domain dictionary.
        
        Returns:
            Domain : Domain class.
        """
        return Domain(**domain_dict)
    

    def to_dict(self):
        """Convert the internal representation into a dict object.

        Returns:
            dict : Dictionary.
        """
        return dict(
            name=self.name,
            dx=self.dx,
            lat_min=self.lat_min,
            lat_max=self.lat_max,
            lon_min=self.lon_min,
            lon_max=self.lon_max
        )
    

    def from_directive(directive):
        """Generate domain object from a directive.

        Example: 'Antarctica,1.0,-180.0,180.0-90.0,-50.0'

        Args:
            directive (str): Domain directive.
        
        Returns:
            Domain : Domain class.
        """
        name, *directives = directive.split(',')
        dx, lat_min, lat_max, lon_min, lon_max = map(float, directives)
        return Domain(
            name=name,
            dx=dx,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max
        )
    

    def to_directive(self):
        """Convert the internal representation into a directive string.

        Returns:
            str : Directive.
        """
        return f'{self.name},{self.dx},{self.lat_min},{self.lat_max},{self.lon_min},{self.lon_max}'
    

    def subset_xarray(self, ds, drop=True):
        """Subset an xarray object with this domain object.

        Args:
            ds (xarray.Dataset or xarray.DataArray)
            drop (bool) : Drop the data outside of the domain. Defaults to True.
        
        Returns:
            xarray.Dataset or xarray.DataArray : Object subset with this domain.
        """
        lat_constraint = (ds.lat >= self.lat_min) & (ds.lat <= self.lat_max)
        
        # Fix to cross the meridian
        if self.lon_max < self.lon_min:
            lon_constraint = (ds.lon <= self.lon_min) | (ds.lon >= self.lon_max)
        else:
            lon_constraint = (ds.lon >= self.lon_min) & (ds.lon <= self.lon_max)
        
        constraint = lon_constraint & lat_constraint
        return ds.where(constraint, drop=drop)
    

    def from_config(key, config):
        """Generate a domain object a section in a parsed configparser config object.

        Args:
            key (str): Key of the domain in the section.
            config (configparser.Config) : Configuration object.
        """
        # Get the section
        elements = {key: float(value) for key, value in config[key].items()}
        return Domain(
            name=key,
            **elements
        )