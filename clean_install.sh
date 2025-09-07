#This scripts allow install the library in your computer

rm -rf dist build *.egg-info
python -m pip install --upgrade build twine
python -m build
pip install --force-reinstall dist/dynamodb_parallel_scanx-0.1.0-py3-none-any.whl
dpscan -h
