from setuptools import setup

setup(
    name='BC2-DATA',
    version='2.0',
    description='File system storage based in xarray',
    author='Joseph Nowak',
    author_email='josephgonowak97@gmail.com',
    license='Bita GmbH copyright',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: General',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='Files Xarray Handler Netcdf4 Store Read Write Append Update',
    packages=[
        'store_core',
        'store_core.data_handler',
        'store_core.netcdf_handler',
        'store_core.base_handler',
        'tests'
    ],
    install_requires=[
        'pandas',
        'xarray',
        'netCDF4',
        'numpy',
        'loguru',
        'dask',
        'boto3'
    ]
)
