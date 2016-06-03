# DataPkg

[![anaconda](https://anaconda.org/ostrokach/datapkg/badges/version.svg)](https://anaconda.org/ostrokach/datapkg)
[![docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&?version=latest)](http://ostrokach.github.io/datapkg)
[![travis](https://img.shields.io/travis/ostrokach/datapkg.svg?style=flat-square)](https://travis-ci.org/ostrokach/datapkg)
[![codecov](https://img.shields.io/codecov/c/github/ostrokach/datapkg.svg?style=flat-square)](https://codecov.io/gh/ostrokach/datapkg)

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
