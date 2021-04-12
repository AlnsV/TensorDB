from setuptools import setup


with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='TensorDB',
    version='1.0',
    description='Database based in a file system storage combined with Xarray and Zarr',
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
    keywords='Database Files Xarray Handler Zarr Store Read Write Append Update Upsert Backup Delete S3',
    packages=[
        'tensor_db',
        'tensor_db.file_handlers',
        'tensor_db.backup_handlers'
    ],
    install_requires=required
)
