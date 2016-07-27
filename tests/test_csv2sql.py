import os
import os.path as op
import logging
import subprocess
import shlex
import pandas as pd
import pytest
import sqlalchemy as sa
from datapkg import get_tablename

logger = logging.getLogger(__name__)

DB_SCHEMA = 'testing'

file_db_list = [
    (op.join(op.abspath(op.splitext(__file__)[0]), 'CosmicCellLineProject.tsv.gz'),
     os.environ['DATAPKG_CONNECTION_STRING'] + '/' + DB_SCHEMA),
    (op.join(op.abspath(op.splitext(__file__)[0]), 'CosmicNonCodingVariants.vcf.gz'),
     os.environ['DATAPKG_CONNECTION_STRING'] + '/' + DB_SCHEMA),
]
engine = sa.create_engine(os.environ['DATAPKG_CONNECTION_STRING'])
engine.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(DB_SCHEMA))
engine = sa.create_engine(os.environ['DATAPKG_CONNECTION_STRING'] + '/' + DB_SCHEMA)


@pytest.fixture(scope='session', params=file_db_list)
def file_db(request):
    return request.param


def test_cli(file_db):
    """Test running csv2sql CLI."""
    file, connection_string = file_db
    system_command = (
        'datapkg file2db --file {} --db {} --debug'
        .format(file, connection_string)
    )
    sp = subprocess.run(
        shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.debug(sp.stdout.decode())
    logger.debug(sp.stderr.decode())
    logger.debug(sp.returncode)
    assert sp.returncode == 0

    tablename = get_tablename(file)
    logger.debug("tablename: '{}'".format(tablename))
    df = pd.read_sql_table(tablename, engine)
    df2 = pd.read_csv(op.join(op.splitext(__file__)[0], op.splitext(file)[0] + '.db.gz'))
    # Hacky thing with integer columns
    for c in ['grch', 'fathmm_score', 'patient_age']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c])
    assert (df.fillna(0) == df2.fillna(0)).all().all()
