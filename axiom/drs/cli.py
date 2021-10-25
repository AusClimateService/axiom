def parse_domain(directive):
    """Parse a domain directive.

    Args:
        directive (str) : Domain directive of the form name,dx,lat_min,lat_max,lon_min,lon_max.

    Returns:
        dict : Domain specification.

    Raises:
        AssertionError : When the directive is missing componenents.
        TypeError : When the directive is unable to be parsed.
    """
    segments = directive.split(',')
    assert len(segments) == 6
    name = segments[0]
    dx, lat_min, lat_max, lon_min, lon_max = [float(s) for s in segments[1:]]

    # Return a dictionary of the parsed information.
    return dict(
        name=name,
        dx=dx,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max
    )
