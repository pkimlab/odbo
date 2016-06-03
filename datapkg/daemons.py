""".

TODO: We should probably run this in a subprocess, so that `mysqld` logs
appear concomitant with database commands.
"""
import sys
import os.path as op
import time
import tempfile
import subprocess
import shlex
import logging
import atexit

logger = logging.getLogger(__name__)


class MySQLDaemon:

    def __init__(self, *, basedir=None, datadir=None):
        if basedir is None:
            basedir = op.dirname(op.dirname(sys.executable))
            logger.debug("'basedir': {}".format(basedir))
        self.basedir = basedir
        if datadir is None:
            datadir = op.join(tempfile.gettempdir(), 'mysql_db')
            logger.debug("'datadir': {}".format(datadir))
        self.datadir = datadir
        # Working variables
        self._mysqld_process = None

    def install_db(self):
        system_command = """\
mysql_install_db --no-defaults --basedir={basedir} --datadir={datadir} \
""".format(basedir=self.basedir, datadir=self.datadir)
        logger.debug('===== Initializing MySQL database... =====')
        logger.debug(system_command)
        p = subprocess.Popen(
            shlex.split(system_command),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1)
        for line in p.stdout:
            line = line.strip()
            if ' [Note] ' in line:
                line = line.partition(' [Note] ')[-1]
            logger.debug(line)
            if not line:
                logger.debug("Reached an empty line!")
                return

    def start(self):
        if self._mysqld_process is not None:
            logger.info("MySQL is already running (pid: {})".format(self._mysqld_process.pid))
        logger.debug('===== Starting MySQL daemon... =====')
        system_command = """\
    mysqld --no-defaults --basedir={basedir} --datadir={datadir} \
    """.format(basedir=self.basedir, datadir=self.datadir)
        logger.debug(system_command)
        self._mysqld_process = subprocess.Popen(
            shlex.split(system_command),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1)
        for line in self._iter_stdout(self._mysqld_process):
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
        for line in self._iter_stdout(self._mysqld_process):
            logger.debug(line)
        logger.debug('mysqld poll: {}'.format(self._mysqld_process.poll()))
        logger.debug('mysqld returncode: {}'.format(self._mysqld_process.returncode))
        assert self._mysqld_process.poll() is not None
        self._mysqld_process = None

    def _iter_stdout(self, p):
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
