import os
import os.path as op
import tempfile
import odbo
import logging
import psutil
import subprocess
import shlex
import pytest
import shutil
import pandas as pd
from odbo import get_tablename

logger = logging.getLogger(__name__)


class TestMySQL:

    @classmethod
    def setup_class(cls):
        """Copy-paste from `test_daemons`."""
        cls.tempdir = tempfile.mkdtemp()
        datadir = op.join(cls.tempdir, 'mysql_db')
        os.makedirs(datadir, exist_ok=True)
        db_socket = op.abspath(op.join(cls.tempdir, 'mysql.sock'))
        # Start MariaDB
        mysqld = odbo.MySQLDaemon(
            datadir=datadir,
            db_socket=db_socket,
        )
        mysqld.install_db()
        mysqld.start()
        mysqld.allow_external_connections()
        # Save state
        cls.mysqld = mysqld
        cls.pid = mysqld._mysqld_process.pid
        logger.debug(
            "Process id: {}, status: {}".format(
                cls.pid, psutil.Process(cls.pid).status()))

    @classmethod
    def teardown_class(cls):
        cls.mysqld.stop()
        assert cls.mysqld._mysqld_process is None
        shutil.rmtree(cls.tempdir)
        with pytest.raises(psutil.NoSuchProcess):
            psutil.Process(cls.pid)

    def setup_method(self, method):
        db_schema = 'testing'
        shared_folder = op.join(self.tempdir, 'share')
        connection_string = self.mysqld.get_connection_string(db_schema)
        logger.debug("connection_string: {}".format(connection_string))
        self.db = odbo.MySQLConnection(
            connection_string=connection_string,
            shared_folder=shared_folder,
            storage_host=None,
            echo=False,
            db_engine='MyISAM',
        )

    def test_connection(self):
        self.db.engine.execute("create table xoxo (id int, value varchar(255));")
        self.db.engine.execute("insert into xoxo values (1, 'aaa'), (2, 'bbb');")
        df = pd.read_sql_table('xoxo', self.db.engine)
        df2 = pd.DataFrame([[1, 'aaa'], [2, 'bbb']], columns=['id', 'value'])
        assert (df == df2).all().all()

    @pytest.mark.parametrize("input_file", [
        op.join(op.abspath(op.splitext(__file__)[0]), 'CosmicCellLineProject.tsv.gz'),
        op.join(op.abspath(op.splitext(__file__)[0]), 'CosmicNonCodingVariants.vcf.gz'),
    ])
    def test_csv2sql_cli(self, input_file):
        """Test running csv2sql CLI."""
        system_command = (
            "odbo file2db --file '{}' --db '{}' --debug"
            .format(input_file, self.db.connection_string)
        )
        sp = subprocess.run(
            shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True)
        logger.debug(sp.stdout.strip())
        logger.debug(sp.stderr.strip())
        logger.debug(sp.returncode)
        assert sp.returncode == 0

        tablename = get_tablename(input_file)
        logger.debug("tablename: '{}'".format(tablename))
        df = pd.read_sql_table(tablename, self.db.engine)
        df2 = pd.read_csv(op.join(op.splitext(__file__)[0], op.splitext(input_file)[0] + '.db.gz'))
        # Hacky thing with integer columns
        for c in ['grch', 'fathmm_score', 'patient_age']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c])
        assert (df.fillna(0) == df2.fillna(0)).all().all()
