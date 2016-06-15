import re
import logging
import subprocess
import shlex
from retrying import retry
import paramiko
import sqlalchemy as sa

logger = logging.getLogger(__name__)
logging.getLogger("paramiko").setLevel(logging.WARNING)


def parse_connection_string(connection_string):
    """Split `connection_string` into database parameters.

    Examples
    --------
    >>> from pprint import pprint
    >>> pprint(parse_connection_string('mysql://root:@localhost'))
    {'db_name': '',
     'db_type': 'mysql',
     'host_ip': 'localhost',
     'host_port': '',
     'password': '',
     'socket': '',
     'username': 'root'}
    >>> pprint(parse_connection_string('mysql://root:root_pass@192.168.0.1:3306/test'))
    {'db_name': 'test',
     'db_type': 'mysql',
     'host_ip': '192.168.0.1',
     'host_port': '3306',
     'password': 'root_pass',
     'socket': '',
     'username': 'root'}
    """
    db_params = {}
    (db_params['db_type'], db_params['username'], db_params['password'],
     db_params['host_ip'], db_params['host_port'], db_params['db_name'],
     db_params['socket']) = (
        re.match(
            '^(\w*)://(\w*):(\w*)@(localhost|[0-9\.]*)(|:[0-9]*)(|\/\w*)(|\?unix_socket=.*)$',
            connection_string)
        .groups()
    )
    db_params['host_port'] = db_params['host_port'].strip(':')
    db_params['db_name'] = db_params['db_name'].strip('/')
    db_params['socket'] = db_params['socket'].partition('?unix_socket=')[-1]
    return db_params


def make_connection_string(**vargs):
    if vargs['socket']:
        vargs['socket'] = '?unix_socket=' + vargs['socket']
    connection_string = (
        '{db_type}://{username}:{password}@{host_ip}:{host_port}/{db_name}{socket}'
        .format(**vargs)
    )
    return connection_string


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

    def __init__(self, command, host, stdout, stderr, returncode):
        self.command = command
        self.host = host
        self.stdout = stdout
        self.stderr = stderr
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
def run_command(system_command, host=None, *, shell=False, allowed_returncodes=[0]):
    """Run system command either locally or over ssh."""
    # === Run command ===
    logger.debug(system_command)
    if host is not None:
        logger.debug("Running on host: '{}'".format(host))
        with MySSHClient(host) as ssh:
            _stdin, _stdout, _stderr = ssh.exec_command(system_command)
            stdout = _stdout.read().decode()
            stderr = _stderr.read().decode()
            returncode = _stdout.channel.recv_exit_status()
    else:
        logger.debug("Running locally")
        sp = subprocess.run(
            system_command if shell else shlex.split(system_command),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell, universal_newlines=True,
        )
        stdout = sp.stdout
        stderr = sp.stderr
        returncode = sp.returncode
    # === Process results ===
    stdout_lower = stdout.lower()
    if returncode not in allowed_returncodes:
        error_message = (
            "Encountered an error: '{}'\n".format(stderr) +
            "System command: '{}'\n".format(system_command) +
            "Output: '{}'\n".format(stdout) +
            "Return code: {}".format(returncode)
        )
        logger.error(error_message)
        raise MySubprocessError(
            command=system_command,
            host=host,
            stdout=stdout,
            stderr=stderr,
            returncode=returncode,
        )
    elif 'warning' in stdout_lower or 'error' in stdout_lower:
        logger.warning("Command ran with warnings / errors:\n{}".format(stdout.strip()))
    else:
        logger.debug("Command ran successfully:\n{}".format(stdout.strip()))
    return stdout, stderr, returncode
