# Test
test:
	python setup.py test

# Style
lint:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics