# DataPkg

[![anaconda](https://anaconda.org/kimlab/datapkg/badges/version.svg?style=flat-square)](https://anaconda.org/kimlab/datapkg)
[![docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&?version=latest)](http://kimlaborg.github.io/datapkg)
[![travis](https://img.shields.io/travis/kimlaborg/datapkg.svg?style=flat-square)](https://travis-ci.org/kimlaborg/datapkg)
[![codecov](https://img.shields.io/codecov/c/github/kimlaborg/datapkg.svg?style=flat-square)](https://codecov.io/gh/kimlaborg/datapkg)

DataPkg is a tool to simplify the distribution of pandas DataFrames as CSV and database files.

It analyses a text file and spits out SQL that creates a database table and loads data into that table.


## Examples

```bash
mysql_install_db --no-defaults --basedir=$HOME/anaconda --datadir=$HOME/tmp/mysql_db
mysqld --no-defaults --basedir=$HOME/anaconda --datadir=$HOME/tmp/mysql_db
```

## TODO

- [ ] PostgreSQL support.
- [ ] HDF5 support.
- [ ] MariaDB CollumnStore support.


## Contributing

- Make sure all tests pass before merging into master.
- Follow the PEP8 / PyFlake / Flake8 / etc. guidelines.
- Add tests for new code.
- Try to document things.
- Break any / all of the above if you have a good reason.
