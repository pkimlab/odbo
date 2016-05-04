import os
import os.path as op
import subprocess
import shlex
import pytest
import sqlalchemy as sa
from datapkg import csv2sql


engine = sa.create_engine(os.environ['DATAPKG_CONNECTION_STR'])
engine.execute('CREATE SCHEMA IF NOT EXISTS test')


file_db_list = [
    (op.join(op.abspath(op.splitext(__file__)[0]), 'CosmicCellLineProject.tsv.gz'),
     os.environ['DATAPKG_CONNECTION_STR'] + '/test'),
    (op.join(op.abspath(op.splitext(__file__)[0]), 'CosmicNonCodingVariants.vcf.gz'),
     os.environ['DATAPKG_CONNECTION_STR'] + '/test'),
]


@pytest.fixture(scope='session', params=file_db_list)
def file_db(request):
    return request.param


def test_module(file_db):
    """Test calling csv2sql module."""
    path, connection_string = file_db
    csv2sql.main(path, connection_string)


def test_cli(file_db):
    """Test running csv2sql CLI."""
    path, connection_string = file_db
    system_command = (
        'datapkg csv2sql --file {} --db {} --debug'
        .format(path, connection_string)
    )
    sp = subprocess.run(
        shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(sp.stdout.decode())
    print(sp.stderr.decode())
    return sp


if __name__ == '__main__':
    import pytest
    pytest.main([__file__, '-svx', '--quick'])
