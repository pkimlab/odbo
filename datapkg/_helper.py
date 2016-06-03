import os.path as op
import re
import logging
import time
import contextlib
import subprocess
import shlex
from retrying import retry
import paramiko
import sqlalchemy as sa

logger = logging.getLogger(__name__)
logging.getLogger("paramiko").setLevel(logging.WARNING)

#: IP address of the NFS server
#: (so that you don't have to uncompress files over the network)
STG_HOST = None


def parse_connection_string(connection_string):
    """Split `connection_string` into database parameters.

    Examples
    --------
    >>> parse_connection_string('mysql://root:@localhost') == {\
        'db_name': '',\
        'db_type': 'mysql',\
        'host_ip': 'localhost',\
        'host_port': '',\
        'password': '',\
        'username': 'root'\
    }
    True
    >>> parse_connection_string('mysql://root:root_pass@192.168.0.1:3306/test') == {\
        'db_name': 'test',\
        'db_type': 'mysql',\
        'host_ip': '192.168.0.1',\
        'host_port': '3306',\
        'password': 'root_pass',\
        'username': 'root'\
    }
    True
    """
    db_params = {}
    (db_params['db_type'], db_params['username'], db_params['password'],
     db_params['host_ip'], db_params['host_port'], db_params['db_name']) = (
        re.match(
            '^(\w*)://(\w*):(\w*)@(localhost|[0-9\.]*)(|:[0-9]*)(|\/\w*)$',
            connection_string)
        .groups()
    )
    db_params['host_port'] = db_params['host_port'].strip(':')
    db_params['db_name'] = db_params['db_name'].strip('/')
    return db_params


@contextlib.contextmanager
def decompress(filepath, keep_decompressed=False, stg_host=None, force=True):
    filepath_tsv, ext = op.splitext(filepath)
    if op.isfile(filepath_tsv) and not force:
        logger.info("Uncompressed file already exits: {}".format(filepath_tsv))
        yield filepath_tsv
        return
    # File not compressed, do nothing
    if ext not in ['.gz', '.bz2']:
        logger.debug("File '{}' is not compressed...".format(filepath_tsv))
        yield filepath
        return
    # File compressed
    try:
        logger.debug("Uncompressing file...".format(filepath))
        system_command = "7za x -bd -o'{}' '{}'".format(op.dirname(filepath), filepath)
        run_command(system_command, host=stg_host)
        n_tries = 0
        while n_tries < 10:
            if op.isfile(filepath_tsv):
                break
            else:
                print("Waiting for the decompressed file to 'appear'...")
                time.sleep(n_tries * 10)
                n_tries += 1
        assert op.isfile(filepath_tsv)
        yield filepath_tsv
    except Exception as e:
        logger.error('{}: {}'.format(type(e), e))
        raise e
    finally:
        if not keep_decompressed:
            logger.debug("Removing decompressed file '{}'...".format(filepath_tsv))
            system_command = "rm -f '{}'".format(filepath_tsv)
            run_command(system_command)
            assert not op.isfile(filepath_tsv)


# === Retrying ===
def _check_exception(exc, valid_exc):
    logger.error('The following exception occured:\n{}'.format(exc))
    to_retry = isinstance(exc, valid_exc)
    if to_retry:
        logger.error('Retrying...')
    return to_retry


def retry_database(fn):
    """Decorator to keep probing the database untill you succeed."""
    r = retry(
        retry_on_exception=lambda exc:
            _check_exception(exc, valid_exc=sa.exc.OperationalError),
        wait_exponential_multiplier=1000,
        wait_exponential_max=60000,
        stop_max_attempt_number=7)
    return r(fn)


def rety_subprocess(fn):
    r = retry(
        retry_on_exception=lambda exc:
            _check_exception(exc, valid_exc=MySubprocessError),
        wait_exponential_multiplier=1000,
        wait_exponential_max=60000,
        stop_max_attempt_number=7)
    return r(fn)


class MySubprocessError(subprocess.SubprocessError):

    def __init__(self, command, host, output, returncode):
        self.command = command
        self.host = host
        self.output = output
        self.returncode = returncode


# === Run command ====
class MySSHClient:

    def __init__(self, ssh_host):
        self.ssh_host = ssh_host
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def __enter__(self):
        logger.debug("Initializing SSH client: '{}'".format(self.ssh_host))
        self.ssh.connect(self.ssh_host)
        return self.ssh

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.ssh.close()
        if exc_type or exc_value or exc_tb:
            import traceback
            logger.error(exc_type)
            logger.error(exc_value)
            logger.error(traceback.print_tb(exc_tb))
            return True
        else:
            return False


@rety_subprocess
def run_command(system_command, host=None):
    """Run system command either locally or over ssh."""
    logger.debug(system_command)
    if host is not None:
        logger.debug("Running on host: '{}'".format(host))
        with MySSHClient(host) as ssh:
            stdin, stdout, stderr = ssh.exec_command(system_command)
            output = stdout.read().decode() + stderr.read().decode()
            returncode = stdout.channel.recv_exit_status()
    else:
        logger.debug("Running locally")
        sp = subprocess.run(
            shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output = sp.stdout + sp.stderr
        returncode = sp.returncode
    # Process results
    if returncode != 0:
        error_message = (
            "Encountered an error: '{}'\n".format(output) +
            "System command: '{}'\n".format(system_command) +
            "Return code: {}".format(returncode)
        )
        logger.error(error_message)
        raise MySubprocessError(
            command=system_command,
            host=host,
            output=output,
            returncode=returncode,
        )
    logger.debug("Command ran successfully!")
    logger.debug("output: {}".format(output))
