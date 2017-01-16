# odbo

[![anaconda](https://anaconda.org/kimlab/odbo/badges/version.svg?style=flat-square)](https://anaconda.org/kimlab/odbo)
[![docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&?version=latest)](http://kimlaborg.github.io/odbo)
[![travis](https://img.shields.io/travis/kimlaborg/odbo.svg?style=flat-square)](https://travis-ci.org/kimlaborg/odbo)
[![codecov](https://img.shields.io/codecov/c/github/kimlaborg/odbo.svg?style=flat-square)](https://codecov.io/gh/kimlaborg/odbo)

odbo is a tool to simplify the distribution of pandas DataFrames as CSV and database files.

It analyses a text file and spits out SQL that creates a database table and loads data into that table.

It is similar to to the blaze [odo](https://github.com/blaze/odo) project, and hence carries a similar name. It was previously called "datapkg", but it seems that this name is [already taken].

## Examples

```bash
mysql_install_db --no-defaults --basedir=$HOME/anaconda --datadir=$HOME/tmp/mysql_db
mysqld --no-defaults --basedir=$HOME/anaconda --datadir=$HOME/tmp/mysql_db
```

## TODO

- [ ] Lower flake8 max-complexity to 10.
- [ ] PostgreSQL support.
- [ ] HDF5 support.
- [ ] MariaDB CollumnStore support.


## Contributing

- Make sure all tests pass before merging into master.
- Follow the PEP8 / PyFlake / Flake8 / etc. guidelines.
- Add tests for new code.
- Try to document things.
- Break any / all of the above if you have a good reason.
