""".

TODO: We should probably run this in a subprocess, so that `mysqld` logs
appear concomitant with database commands.
"""
import sys
import os
import os.path as op
import tempfile
import logging
import atexit
import socket

from kmtools.db_tools import make_connection_string
from datapkg import utils

logger = logging.getLogger(__name__)


class _Daemon:

    def get_connection_string(self, db_schema=None, db_url='localhost'):
        """Return database connection string (e.g. for sqlalchemy).

        Parameters
        ----------
        db_schema : db_schema
            Default database schema for the connection.
        db_url : str
            The IP address / domain name of the computer running the database server.
            With MySQL, if db_url == 'localhost', the client will try to establish
            the connection using a Unix domain socket instead.
        """
        # Set `db_url`
        if db_url is None:
            db_url = socket.gethostbyname(socket.gethostname())

        # Set `db_socket` if neccessary
        if db_url == 'localhost':
            db_socket = self.db_socket
            assert db_socket
        else:
            db_socket = None

        return make_connection_string(
            db_type=self.db_type,
            db_username='root',
            db_url=db_url,
            db_port=str(self.db_port),  # TODO: remove str() cast when updated
            db_schema=db_schema,
            db_socket=db_socket,
        )


# === MySQL / MariaDB ===

class MySQLDaemon(_Daemon):

    db_type = 'mysql'
    _default_storage_engine = 'MyISAM'

    def __init__(self, *, basedir=None, datadir=None, db_socket=None, db_port=9306):
        if basedir is None:
            basedir = op.dirname(op.dirname(sys.executable))
            logger.debug("'basedir': {}".format(basedir))
        self.basedir = basedir
        if datadir is None:
            datadir = op.join(tempfile.gettempdir(), 'mysql_db')
            logger.debug("'datadir': {}".format(datadir))
        self.datadir = datadir
        if db_socket is None:
            db_socket = op.join(tempfile.gettempdir(), 'mysql.sock')
            logger.debug("'db_socket': {}".format(db_socket))
        self.db_socket = db_socket
        self.db_port = db_port
        # Working variables
        self._mysqld_process = None

    def install_db(self):
        log_files = [op.join(self.datadir, x) for x in ['ib_logfile0', 'ib_logfile1']]
        for log_file in log_files:
            if op.isfile(log_file):
                os.remove(log_file)
        system_command = """\
mysql_install_db --no-defaults --basedir={basedir} --datadir={datadir} \
""".format(basedir=self.basedir, datadir=self.datadir)
        logger.debug('===== Initializing MySQL database... =====')
        logger.debug(system_command)
        p = utils._start_subprocess(system_command)
        for line in utils._iter_stdout(p):
            logger.debug(line)

    def _format_kwargs(self, **kwargs):
        """
        Examples
        --------
        >>> mysqld = MySQLDaemon()
        >>> sorted(mysqld._format_kwargs(aaa=None, bbb='xxx', ccc=300).split())
        ['--aaa', '--bbb=xxx', '--ccc=300']
        """
        kwargs_string = ''
        for x, y in kwargs.items():
            if y is None:
                kwargs_string += ' --{}'.format(x)
            else:
                kwargs_string += ' --{}={}'.format(x, y)
        return kwargs_string

    def start(
            self,
            default_storage_engine=None,
            external_locking=None,
            innodb_fast_shutdown=None,
            open_files_limit=4096,
            max_connections=150,
            **kwargs):
        if default_storage_engine is None:
            default_storage_engine = self._default_storage_engine
        if self._mysqld_process is not None:
            logger.info(
                "MySQL is already running (pid: {}, socket: '{}')."
                .format(self._mysqld_process, self.db_socket))
            return

        logger.debug('===== Starting MySQL daemon... =====')
        # --delay-key-write=OFF --query-cache-size=0
        system_command = """\
mysqld --no-defaults --basedir={basedir} --datadir={datadir} \
    --socket='{db_socket}' --port={db_port} \
    --max_connections={max_connections} \
    --open_files_limit={open_files_limit} \
    --default_storage_engine={default_storage_engine} \
    {kwargs} \
""".format(
            basedir=self.basedir,
            datadir=self.datadir,
            db_socket=self.db_socket,
            db_port=self.db_port,
            max_connections=max_connections,
            open_files_limit=open_files_limit,
            default_storage_engine=default_storage_engine,
            kwargs=self._format_kwargs(**kwargs),
        )

        logger.debug(system_command)
        self._mysqld_process = utils._start_subprocess(system_command)
        for line in utils._iter_stdout(self._mysqld_process):
            logger.debug(line)
            if 'mysqld: ready for connections' in line:
                break
        # Stop MySQL when you exit Python
        atexit.register(self.stop)

    def stop(self):
        if self._mysqld_process is None:
            logger.debug("MySQL daemon is already shut down!")
            return
        self._mysqld_process.terminate()
        for line in utils._iter_stdout(self._mysqld_process):
            logger.debug(line)
        logger.debug('mysqld poll: {}'.format(self._mysqld_process.poll()))
        logger.debug('mysqld returncode: {}'.format(self._mysqld_process.returncode))
        assert self._mysqld_process.poll() is not None
        self._mysqld_process = None

    def allow_external_connections(self):
        system_command = """\
mysql -u root --socket {db_socket} -e "\
drop user if exists 'root'@'%';
create user 'root'@'%'; \
grant all on *.* to 'root'@'%'; \
flush privileges;" \
""".format(db_socket=self.db_socket)
        p = utils._start_subprocess(system_command)
        for line in utils._iter_stdout(p):
            logger.debug(line)
