# DataPkg

[![anaconda](https://anaconda.org/pmklab/datapkg/badges/version.svg)](https://anaconda.org/pmklab/datapkg)
[![docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&?version=latest)](http://pmklab.github.io/datapkg)
[![travis](https://img.shields.io/travis/pmklab/datapkg.svg?style=flat-square)](https://travis-ci.org/pmklab/datapkg)
[![codecov](https://img.shields.io/codecov/c/github/pmklab/datapkg.svg?style=flat-square)](https://codecov.io/gh/pmklab/datapkg)

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

- Follow [GitHub Flow](https://guides.github.com/introduction/flow/).
- Make sure all tests pass before merging into master (including PEP8).
- Add tests for your code.
