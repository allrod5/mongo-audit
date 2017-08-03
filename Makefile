.PHONY: install
install:
	@pip install -r requirements.txt

.PHONY: test
test:
	@PYTHONPATH=. pytest --cov-report term --cov-report html:htmlcov --cov=versionedmongo tests
	@flake8

.PHONY: clean
clean:
	@find ./ -type d -name 'htmlcov' -exec rm -rf {} +;
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '*~' -exec rm -f {} \;