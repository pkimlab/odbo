import os
import os.path as op
import tempfile
import datapkg
import logging
import psutil
import pytest
import shutil
import pandas as pd

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
        mysqld = datapkg.MySQLDaemon(
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

    def test_connection(self):
        db_schema = 'testing'
        shared_folder = op.join(self.tempdir, 'share')
        db = datapkg.MySQL(
            connection_string=self.mysqld.get_connection_string(db_schema),
            shared_folder=shared_folder,
            storage_host=None,
            echo=False,
            db_engine='MyISAM',
        )
        db.engine.execute("create table xoxo (id int, value varchar(255));")
        db.engine.execute("insert into xoxo values (1, 'aaa'), (2, 'bbb');")
        df = pd.read_sql_table('xoxo', db.engine)
        df2 = pd.DataFrame([[1, 'aaa'], [2, 'bbb']], columns=['id', 'value'])
        assert (df == df2).all().all()
