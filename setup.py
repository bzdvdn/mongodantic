from os import path
from io import open
from setuptools import setup, find_packages

from mongodantic import __version__


def read(f):
    return open(f, 'r').read()


setup(
    name='mongodantic',
    version=__version__,
    packages=find_packages(),
    install_requires=[
        'pydantic>=1.3,<2',
        'pymongo>=3.10.1,<3.11',
    ],
    description='Mongo ODM, based on pydantic',
    author='bzdvdn',
    author_email='bzdv.dn@gmail.com',
    url='https://github.com/bzdvdn/mongodantic',
    license='MIT',
    python_requires=">=3.6",
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
)
