# harlequin-odbc

This repo provides the ODBC adapter for Harlequin.

## Installation

`harlequin-odbc` depends on `harlequin`, so installing this package will also install Harlequin.

### Pre-requisites

You will need an ODBC driver manager installed on your OS. Windows has one built-in, but for Unix-based OSes, you will need to download and install one before installing `harlequin-odbc`. You can install unixODBC with `brew install unixodbc` or `sudo apt install unixodbc`. See the [pyodbc docs](https://github.com/mkleehammer/pyodbc/wiki/Install) for more info.

Additionally, you will need to install the ODBC driver for your specific database (e.g., `ODBC Driver 18 for SQL Server` for MS SQL Server). For more information, see the docs for your specific database.

### Using pip

To install this adapter into an activated virtual environment:
```bash
pip install harlequin-odbc
```

### Using poetry

```bash
poetry add harlequin-odbc
```

### Using pipx

If you do not already have Harlequin installed:

```bash
pip install harlequin-odbc
```

If you would like to add the ODBC adapter to an existing Harlequin installation:

```bash
pipx inject harlequin harlequin-odbc
```

### As an Extra
Alternatively, you can install Harlequin with the `odbc` extra:

```bash
pip install harlequin[odbc]
```

```bash
poetry add harlequin[odbc]
```

```bash
pipx install harlequin[odbc]
```

## Usage and Configuration

You can open Harlequin with the ODBC adapter by selecting it with the `-a` option and passing an ODBC connection string:

```bash
harlequin -a odbc 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:harlequin-example.database.windows.net,1433;Database=dev;Uid=harlequin;Pwd=my_secret;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
```

The ODBC adapter does not accept other options.

### Catalog search

This adapter implements Harlequin's catalog search, so `hsql` can find an object
by name without walking the catalog one level at a time:

```bash
hsql --catalog-search customer_id -a odbc "${ODBC_CONN_STR}"
```

The search matches a substring of an object's name, case-insensitively, at every
level of the catalog: databases, schemas, relations, and columns. It uses your
driver's ODBC catalog functions -- the same ones that build the Data Catalog --
so it makes one call for the databases, schemas, and relations, plus one call per
database for the columns.

One caveat: the column half of the search hands the term to your driver as an
ODBC search pattern, so a driver that matches patterns case-sensitively can miss
a column whose name differs from the term in case. Every level above a column is
matched by this adapter, and is always case-insensitive.

For more information, see the [Harlequin Docs](https://harlequin.sh/docs/odbc/index).
