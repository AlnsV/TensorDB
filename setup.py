from setuptools import setup

setup(
    name='File DB',
    version='2.0',
    description='File system storage based in Xarray and Zarr',
    author='Joseph Nowak',
    author_email='josephgonowak97@gmail.com',
    license='Bita GmbH copyright',
    classifiers=[
        'Development Status :: 1 - Beta',
        'Intended Audience :: Developers and Economists',
        'Intended Audience :: Science/Research',
        'Intended Audience :: General',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='Files Xarray Handler Zarr Store Read Write Append Update Upsert Backup',
    packages=[
        'store_core',
        'store_core.data_handler',
        'store_core.base_handler',
        'bitacore.file_db',
        'bitacore.relational_handler',
        'tests'
    ],
    install_requires=[
        'pandas',
        'xarray',
        'numpy',
        'loguru',
        'dask',
        'boto3',
        'pyyaml',
        'zarr',
        'fasteners',
        'botocore',
        'pytest'
    ]
)
