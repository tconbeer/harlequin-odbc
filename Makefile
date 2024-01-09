.PHONY: serve
serve:
	harlequin -a odbc "${ODBC_CONN_STR}"

.PHONY: lint
lint:
	black .
	ruff . --fix
	mypy