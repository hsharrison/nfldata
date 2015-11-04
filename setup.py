from glob import glob
from os.path import splitext, basename, join
from setuptools import setup, find_packages

exec(open(join('src', 'nfldata', '__version__.py')).read())

setup(
    name='nfldata',
    version=__version__,
    url='https://github.com/hsharrison/nfldata',
    license='MIT',
    author='Henry S. Harrison',
    author_email='henry.schafer.harrison@gmail.com',
    description='Utilities for handling NFL data.',
    long_description=open('README.md').read(),

    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob(join('src', '*.py'))],
    zip_safe=True,

    install_requires=[
        'toolz',
        'numpy>=1.10',
        'pandas',
        'pyyaml',
        'psycopg2',
        'sqlalchemy',
    ],

    package_data={
        'nfldata': ['data/*.yaml'],
    },
)
