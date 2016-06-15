""".

TODO: We should probably run this in a subprocess, so that `mysqld` logs
appear concomitant with database commands.
"""
import sys
import os
import os.path as op
import time
import tempfile
import subprocess
import shlex
import logging
import atexit

logger = logging.getLogger(__name__)


def _start_subprocess(system_command):
    p = subprocess.Popen(
        shlex.split(system_command),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1)
    return p


def _iter_stdout(p):
    for line in p.stdout:
        line = line.strip()
        if ' [Note] ' in line:
            line = line.partition(' [Note] ')[-1]
        if not line:
            # logger.debug("DONE! (reached an empty line)")
            if p.poll() is not None:
                return
            else:
                time.sleep(0.1)
                continue
        yield line


class MySQLDaemon:

    def __init__(self, *, basedir=None, datadir=None, socket=None, port=3306):
        if basedir is None:
            basedir = op.dirname(op.dirname(sys.executable))
            logger.debug("'basedir': {}".format(basedir))
        self.basedir = basedir
        if datadir is None:
            datadir = op.join(tempfile.gettempdir(), 'mysql_db')
            logger.debug("'datadir': {}".format(datadir))
        self.datadir = datadir
        if socket is None:
            socket = op.join(tempfile.gettempdir(), 'mysql.sock')
            logger.debug("'socket': {}".format(socket))
        self.socket = socket
        self.port = port
        # Working variables
        self._mysqld_process = None

    def install_db(self):
        system_command = """\
mysql_install_db --no-defaults --basedir={basedir} --datadir={datadir} \
""".format(basedir=self.basedir, datadir=self.datadir)
        logger.debug('===== Initializing MySQL database... =====')
        logger.debug(system_command)
        p = _start_subprocess(system_command)
        for line in _iter_stdout(p):
            logger.debug(line)

    def myisampack(self, schema_name):
        data_files = [
            op.abspath(op.join(self.datadir, schema_name, f))
            for f in os.listdir(op.join(self.datadir, schema_name))
            if op.splitext(f)[-1] == '.MYI'
        ]
        data_files_str = " ".join("'{}'".format(op.abspath(f)) for f in data_files)
        # Compress files
        system_command = "myisampack --no-defaults '{}'".format(data_files_str)
        # allowed_returncodes=[0, 2]
        p = _start_subprocess(system_command)
        for line in _iter_stdout(p):
            logger.debug(line)
        # Re-create index
        system_command = "myisamchk -rq '{}'".format(data_files_str)
        p = _start_subprocess(system_command)
        for line in _iter_stdout(p):
            logger.debug(line)

    def get_connection_string(self, db_name):
        connection_string = (
            'mysql://root:@localhost:{port}/{db_name}?unix_socket={socket}'
            .format(port=self.port, socket=self.socket, db_name=db_name)
        )
        return connection_string

    def start(self):
        if self._mysqld_process is not None:
            logger.info(
                "MySQL is already running (pid: {}, socket: '{}')."
                .format(self._mysqld_process, self.socket))
            return
        logger.debug('===== Starting MySQL daemon... =====')
        system_command = """\
mysqld --no-defaults --basedir={basedir} --datadir={datadir} \
    --socket='{socket}' --port={port} \
    --external-locking \
""".format(basedir=self.basedir, datadir=self.datadir, socket=self.socket, port=self.port)
        # --delay-key-write=OFF --query-cache-size=0
        logger.debug(system_command)
        self._mysqld_process = _start_subprocess(system_command)
        for line in _iter_stdout(self._mysqld_process):
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
        for line in _iter_stdout(self._mysqld_process):
            logger.debug(line)
        logger.debug('mysqld poll: {}'.format(self._mysqld_process.poll()))
        logger.debug('mysqld returncode: {}'.format(self._mysqld_process.returncode))
        assert self._mysqld_process.poll() is not None
        self._mysqld_process = None

    def allow_external_connections(self):
        system_command = """\
mysql -u root --socket {socket} -e "\
drop user if exists 'root'@'%';
create user 'root'@'%'; \
grant all on *.* to 'root'@'%'; \
flush privileges;" \
""".format(socket=self.socket)
        p = _start_subprocess(system_command)
        for line in _iter_stdout(p):
            logger.debug(line)
