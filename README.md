

![Tests](https://github.com/egeres/lifetracking/actions/workflows/python-package.yml/badge.svg)


To-do:

- [x] Pre-commit hooks
- [x] Add CI/CD testing on github actions
- [x] Add delay parameter for Node_int
- [ ] First iteration of a csv to df node
- [x] Load timeslice
- [ ] Iterate over caching approaches

- [ ] Should I remove `from __future__ import annotations` and deprecate 3.8?


Setup:

```shell
poetry install
```

Update:

```shell
poetry update
pre-commit autoupdate
pre-commit run --all-files
pytest --disable-pytest-warnings
```
