import xarray as xr
import xclim as xc
import xclim.indices as xci
import xclim.indicators.atmos as xia
import xclim.indicators.cf as xic
from axiom.decorators import metadata


GROWING_SEASON_DATES = dict(sh='01-01', nh='07-01')

@metadata(name='frost_days', climdex='FD')
def fd(tasmin):
    return xci.tn_days_below(tasmin, '0.0 degC', freq='YS')


@metadata(name='summer_days', climdex='SU')
def su(tasmax):
    return xci.tx_days_above(tasmax, '25.0 degC', freq='YS')


@metadata(name='icing_days', climdex='ID')
def id(tasmax):
    return xci.tn_days_below(tasmax, '0.0 degC', freq='YS')


@metadata(name='tropical_nights', climdex='TR')
def tr(tasmin):
    return xci.tropical_nights(tasmin)


@metadata(name='growing_season_length', climdex='GSL')
def gsl(tasmean, hemisphere='sh'):
    return xia.growing_season_length(
        tasmean,
        thresh='5.0 degC',
        window=6,
        mid_date=GROWING_SEASON_DATES[hemisphere],
        freq='YS'
    )


@metadata(name='maximum_value_of_daily_maximum_temperature', climdex='TXx')
def txx(tasmax):
    return xic.txx(tasmax)


@metadata(name='maximum_value_of_daily_minimum_temperature', climdex='TNx')
def tnx(tasmin):
    return xic.tnx(tasmin)


@metadata(name='minimum_value_of_daily_maximum_temperature', climdex='TXn')
def txn(tasmax):
    return xic.tnx(tasmax)


@metadata(name='minimum_value_of_daily_minimum_temperature', climdex='TNn')
def tnn(tasmin):
    return xic.tnn(tasmin)


@metadata(climdex='TN10p')
def tn10p(tasmin):
    pass


@metadata(climdex='TX10p')
def tx10p(tasmax):
    pass


@metadata(climdex='TN90p')
def tn90p(tasmin):
    pass


@metadata(climdex='TX90p')
def tx90p(tasmax):
    pass

@metadata(climdex='WSI')
def wsdi(tasmax):
    pass

@metadata(climdex='CSDI')
def csdi(tasmax):
    pass

@metadata(climdex='DTR')
def dtr(tasmin, tasmax):
    pass

@metadata(climdex='ETR')
def etr(tasmin, tasmax):
    pass

@metadata(climdex='Rx1day')
def rx1day(precip):
    pass

@metadata(climdex='Rx5day')
def rx5day(precip):
    pass

@metadata(climdex='SDII')
def sdii(precip):
    pass

@metadata(climdex='R10mm')
def r10mm(precip):
    pass

@metadata(climdex='R100mm')
def r100mm(precip):
    pass

@metadata(climdex='Rnnmm')
def rnnmm(precip, nn):
    pass

@metadata(climdex='CDD')
def cdd(precip):
    pass

@metadata(climdex='CWD')
def cwd(precip):
    pass

@metadata(climdex='R95p')
def r95p(precip):
    pass

@metadata(climdex='R99p')
def r99p(precip):
    pass

@metadata(climdex='R95pTOT')
def r95ptot(precip):
    pass

@metadata(climdex='R99pTOT')
def r99ptot(precip):
    pass

@metadata(climdex='PRCPTOT')
def prcptot(precip):
    pass