#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from glob import glob
from pathlib import Path, PurePath

from setuptools import find_packages, setup

from src import dbdiff


def read(*names, **kwargs):
    with Path(PurePath.joinpath(Path(__file__).parent, *names)).open(
        encoding=kwargs.get('encoding', 'utf8')
    ) as fh:
        return fh.read()


setup(
    name='dbdiff',
    version=dbdiff.__version__,
    license='MIT',
    description='Compare two tables on Vertica.',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author='Andy Reagan',
    author_email='andy@andyreagan.com',
    url='https://github.com/andyreagan/dbdiff',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[PurePath(path).name.suffix[0] for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Unix',
        'Operating System :: POSIX',
        # 'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        # 'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        # 'Programming Language :: Python :: 3.8',
        # 'Programming Language :: Python :: Implementation :: CPython',
        # 'Programming Language :: Python :: Implementation :: PyPy',
        # uncomment if you test on these interpreters:
        # 'Programming Language :: Python :: Implementation :: IronPython',
        # 'Programming Language :: Python :: Implementation :: Jython',
        # 'Programming Language :: Python :: Implementation :: Stackless',
        'Topic :: Utilities',
    ],
    project_urls={
        # 'Documentation': 'https://dbdiff.readthedocs.io/',
        # 'Changelog': 'https://dbdiff.readthedocs.io/en/latest/changelog.html',
        'Issue Tracker': 'https://github.com/andyreagan/dbdiff/issues',
    },
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    python_requires='>=3.6',
    install_requires=[
        'click',
        'requests',
        'pandas',
        'Jinja2',
        'python-dotenv',
        'vertica_python',
        'xlsxwriter'
        # eg: 'aspectlib==1.1.1', 'six>=1.7',
    ],
    extras_require={
        # eg:
        #   'rst': ['docutils>=0.11'],
        #   ':python_version=="2.6"': ['argparse'],
    },
    entry_points={
        'console_scripts': [
            'dbdiff = dbdiff.cli:main',
        ]
    },
)
