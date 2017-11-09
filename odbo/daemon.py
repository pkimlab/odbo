""".

TODO: We should probably run this in a subprocess, so that `mysqld` logs
appear concomitant with database commands.
"""
import atexit
import logging
import os
import os.path as op
import socket
import sys
import tempfile
from textwrap import dedent

from kmtools.db_tools import ConOpts, make_connection_string
from kmtools.system_tools import iter_stdout, start_subprocess

logger = logging.getLogger(__name__)


def start_database(db_type, *args, **kwargs):
    db_type = db_type.lower()
    if db_type not in ['mysql']:
        raise Exception("Unsupported DB_TYPE = '{}'".format(db_type))
    if db_type == 'mysql':
        return start_mysql_database(*args, **kwargs)


def start_mysql_database(db_data_dir, db_socket, db_port, allow_external_connections=True):
    mysqld = MySQLDaemon(
        datadir=db_data_dir,
        db_socket=db_socket,
        db_port=db_port,
    )
    if not op.exists(os.environ['DB_SOCKET']):
        try:
            logger.info('Starting MySQL database...')
            mysqld.install_db()
            mysqld.start()
            # if allow_external_connections:
            #     mysqld.allow_external_connections()
        except Exception as e:
            logger.error("Failed to start database beacuse of error:\n    {}: {}".format(
                type(e), e))
            mysqld.stop()
    else:
        logger.info('MySQL database already running...')
    return mysqld


class _Daemon:

    def get_connection_string(self, db_schema=None, db_url='localhost') -> str:
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
            ConOpts(self.db_type, 'root', 'rootpass', db_url, self.db_port, db_schema, db_socket))


# === MySQL / MariaDB ===


class MySQLDaemon(_Daemon):
    """MySQL deamon.

    .. note::
        For best OLAP performance, you should disable InnoDB and use only MyISAM (or Aria) tables,
        setting ``key_buffer_size`` as large as possible.

    """
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
        system_command = dedent(f"""\
            mysql_install_db --no-defaults --basedir={self.basedir} --datadir={self.datadir}
            """).strip()
        logger.debug('===== Initializing MySQL database... =====')
        logger.debug(system_command)
        p = start_subprocess(system_command)
        for line in iter_stdout(p):
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
                kwargs_string += f' --{x}'
            else:
                kwargs_string += f' --{x}={y}'
        return kwargs_string

    def start(self,
              default_storage_engine=None,
              external_locking=None,
              innodb_fast_shutdown=None,
              open_files_limit=4096,
              max_connections=150,
              **kwargs):
        if default_storage_engine is None:
            default_storage_engine = self._default_storage_engine
        if self._mysqld_process is not None:
            logger.info("MySQL is already running (pid: {}, socket: '{}').".format(
                self._mysqld_process.pid, self.db_socket))
            return

        init_file = tempfile.NamedTemporaryFile()
        init_file_contents = dedent("""\
            -- Create root password
            ALTER USER 'root'@'localhost' IDENTIFIED BY 'rootpass';

            -- Allow external connections
            DROP USER IF EXISTS 'root'@'%';
            CREATE USER 'root'@'%' IDENTIFIED BY 'rootpass';
            GRANT ALL ON *.* TO 'root'@'%';
            FLUSH PRIVILEGES;
            """)
        with open(init_file.name, 'wt') as ofh:
            ofh.write(init_file_contents)

        logger.debug('===== Starting MySQL daemon... =====')
        # --delay-key-write=OFF --query-cache-size=0
        system_command = dedent(f"""\
            mysqld --no-defaults --basedir={self.basedir} --datadir={self.datadir}
                --socket='{self.db_socket}' --port={self.db_port}
                --max_connections={max_connections}
                --open_files_limit={open_files_limit}
                --default_storage_engine={default_storage_engine}
                --key_buffer_size=1073741824
                --init-file='{init_file.name}'
                {self._format_kwargs(**kwargs)}
            """).replace('\n', ' ')
        logger.debug(system_command)
        self._mysqld_process = start_subprocess(system_command)
        for line in iter_stdout(self._mysqld_process):
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
        for line in iter_stdout(self._mysqld_process):
            logger.debug(line)
        logger.debug('mysqld poll: {}'.format(self._mysqld_process.poll()))
        logger.debug('mysqld returncode: {}'.format(self._mysqld_process.returncode))
        assert self._mysqld_process.poll() is not None
        self._mysqld_process = None
