pyenv global 3.7.13 3.8.13 3.9.12 3.10.4
python3 setup.py sdist -d dist
python3 -m twine check -- dist/*.tar.gz
python3 -m tox --installpkg dist/*.tar.gz
python3 -m twine upload dist/*
pyenv global 3.10.4