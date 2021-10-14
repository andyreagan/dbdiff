# This module is used by setup.py to pull the version below.
# WARNING: this approach will fail if we import anything here that
# we rely on setup.py to install.
# See that warning on Step 6 here:
# https://packaging.python.org/guides/single-sourcing-package-version/
# If we want to do imports here, there is a different approach.
__version__ = '0.5.6'
