
![Tests](https://github.com/egeres/lifetracking/actions/workflows/python-package.yml/badge.svg)

Setup:

```shell
poetry install
pre-commit install
pre-commit install --hook-type commit-msg
```

Update:

```shell
poetry update
pre-commit autoupdate
pre-commit run --all-files
pytest -n auto --disable-pytest-warnings
coverage run -m pytest --disable-pytest-warnings
coverage report -m
ruff . --watch --ignore=F401
flake8 . --count --max-complexity=10 --max-line-length=88 --statistics --ignore=F401,W503
```
