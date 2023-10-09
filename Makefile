dev:
	python -m venv .venv
	. .venv/bin/activate
	pip install '.[dev]'

install:
	pip install .

uninstall:
	pip uninstall azure_dbr_scim_sync

fmt:
	yapf -pri azure_dbr_scim_sync tests setup.py
	autoflake -ri azure_dbr_scim_sync tests
	isort azure_dbr_scim_sync tests

fmte:
	yapf -pri examples
	autoflake -ri examples
	isort examples

lint:
	pycodestyle azure_dbr_scim_sync
	autoflake --check-diff --quiet --recursive azure_dbr_scim_sync

test:
	pytest --cov=azure_dbr_scim_sync --cov-report html:coverage/html --cov-report xml:coverage/xml --junitxml=.junittest.xml tests/L1* tests/L2* tests/L3*


benchmark:
	pytest --cov=azure_dbr_scim_sync tests/L4*

coverage: test
	open htmlcov/index.html

dist:
	python setup.py bdist_wheel sdist

clean: uninstall
	rm -fr dist *.egg-info .pytest_cache build coverage .junittest*.xml coverage.xml .coverage
