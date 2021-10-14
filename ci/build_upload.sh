python3 setup.py sdist -d dist
python3 -m twine check -- dist/*.tar.gz
python3 -m tox --installpkg dist/*.tar.gz
python3 -m twine upload dist/*
