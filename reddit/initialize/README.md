This directory is for going from a directory of comments to a spot index.  It requires an installation of the "spot" module.

	$ cd /../../somewhere/../..
	$ git clone https://github.com/evangambit/spot
	$ cd spot
	$ pip install .

Then cd into the directory above this one and run

	$ python Initialize/create_spot_index.py

This is necessary when (e.g.) tokenization code changes.