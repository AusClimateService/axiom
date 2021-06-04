from setuptools import setup, find_packages

setup(
    name='axiom',
    version='0.1.0',
    author='Ben Schroeter',
    author_email='ben.schroeter@csiro.au',
    install_requires=[
        'xarray',
        'netCDF4',
        'cerberus',
        'tabulate'
    ],
    scripts=['bin/axiom'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    packages=find_packages()
)
