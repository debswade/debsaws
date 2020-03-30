import sys
from setuptools import setup, find_packages

install_requires=['requests',
    'pymysql',
    'botocore',
    'boto3',
    'paramiko',
    'sshtunnel']

setup(
    name='eventlog-migration',
    version='0.0.1',
    # url='https://github.com/ConnectedHomes/eventlog-migration',
    author='Deborah Balm',
    author_email='deborah.balm@hivehome.com',
    packages=find_packages(),
    install_requires=install_requires
)
