# setup.py
import setuptools
REQUIRED_PACKAGES = ['jaydebeapi','SQLAlchemy','gcsfs','fsspec']
PACKAGE_NAME = 'sql_server_to_bq_package'
PACKAGE_VERSION = '0.0.1'

setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Sql Server to BQ Dependency Packages',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
