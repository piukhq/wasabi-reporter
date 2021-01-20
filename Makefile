lint:
	pipenv run black -t py38 -l 120 .
	pipenv run isort .
	pipenv run flake8 --max-line-length=150

