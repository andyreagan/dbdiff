#!/usr/bin/env python
from glob import glob
from pathlib import Path, PurePath

from setuptools import find_packages, setup

from src import dbdiff


def read(*names, **kwargs):
    with Path(PurePath.joinpath(Path(__file__).parent, *names)).open(
        encoding=kwargs.get("encoding", "utf8")
    ) as fh:
        return fh.read()


setup(
    name="dbdiff",
    version=dbdiff.__version__,
    license="MIT",
    description="Compare two tables on Vertica.",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="Andy Reagan",
    author_email="andy@andyreagan.com",
    url="https://github.com/andyreagan/dbdiff",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[PurePath(path).stem for path in glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Utilities",
    ],
    project_urls={
        "Issue Tracker": "https://github.com/andyreagan/dbdiff/issues",
    },
    keywords=[],
    python_requires=">=3.9",
    install_requires=[
        "click",
        "requests",
        "pandas>=1.0.0",
        "Jinja2",
        "python-dotenv",
        "vertica_python",
        "xlsxwriter",
    ],
    extras_require={},
    entry_points={
        "console_scripts": [
            "dbdiff = dbdiff.cli:cli",
        ]
    },
)
