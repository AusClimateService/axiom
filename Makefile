# Test
test:
	pytest

# Style
lint:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

# Build for PyPi
pypi_build:
	python3 -m build

# Publish to PyPi (test)
test_pypi_publish: pypi_build
	python3 -m twine upload --repository testpypi dist/*

# Publish to PyPi (production)
pypi_publish: pypi_build
	python3 -m twine upload dist/*

