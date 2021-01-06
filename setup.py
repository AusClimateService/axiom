from setuptools import setup

setup(
    name='axiom',
    version='0.1.0',
    author='Ben Schroeter',
    author_email='ben.schroeter@csiro.au',
    install_requires=[
        'xarray',
        'netCDF4'
    ],
    scripts=['bin/axv']
)
