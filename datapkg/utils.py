import os.path as op
import string
import logging
import time
import re
import math
import pandas as pd
from sqlalchemy.dialects.mysql import INTEGER, DOUBLE, BOOLEAN, VARCHAR, MEDIUMTEXT
import subprocess
import shlex
from retrying import retry
import paramiko
import sqlalchemy as sa

logger = logging.getLogger(__name__)
logging.getLogger("paramiko").setLevel(logging.WARNING)

INTEGER = INTEGER()
DOUBLE = DOUBLE()
VARCHAR_SHORT = VARCHAR(32)
VARCHAR_MEDIUM = VARCHAR(255)
TEXT = MEDIUMTEXT()
BOOLEAN = BOOLEAN()

#: IP address of the NFS server
#: (so that you don't have to uncompress files over the network)
STG_HOST = None

#: Extensions that get stripped when creating a database table from a text file
REMOVED_EXTENSIONS = ['.gz', '.tsv', '.csv', '.txt', '.vcf']


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
            if p.poll() is None:
                time.sleep(0.1)
                continue
            else:
                # logger.debug("DONE! (reached an empty line)")
                return
        yield line


def format_unprintable(string):
    r"""Escape tabs (\t), newlines (\n), etc. for system commands and printing.

    Examples
    --------
    >>> format_unprintable('\t')
    '\\t'
    """
    return repr(string).strip("'")


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


def get_tablename(file):
    """Create a proper tablename based on filename.

    Examples
    --------
    >>> get_tablename('/home/username/myFancyFile!!!.txt')
    'my_fancy_file'
    """
    name = op.basename(file)
    while True:
        basename, ext = op.splitext(name)
        if ext in REMOVED_EXTENSIONS:
            name = basename
            continue
        else:
            break
    name = _format_column(name)
    return name


def format_columns(columns):
    """Convert CamelCase and other weird column name formats to pot_hole case.

    Examples
    --------
    >>> format_columns('HelloWorld')
    'hello_world'
    >>> format_columns('Hello_World')
    'hello_world'
    >>> format_columns('hg19_pos(1-based)')
    'hg19_pos_1based'
    >>> format_columns(['Hello (World?)', 'allGood?'])
    ['hello_world', 'all_good']
    """
    single = False
    if isinstance(columns, str):
        single = True
        columns = [columns]
    new_columns = [_format_column(c) for c in columns]
    if single:
        assert len(new_columns) == 1
        return new_columns[0]
    return new_columns


def _format_column(name):
    name = name.replace(' ', '_')
    name = name.replace('(', '_').replace(')', '')
    name = name.replace('%', 'pc')
    keywords = [
        'uniprot', 'grch', 'refseq',
    ]
    for keyword in keywords:
        while keyword in name.lower() and keyword not in name:
            start = name.lower().index(keyword)
            end = start + len(keyword)
            name = name[:start] + keyword + name[end:]
    if name.lower() in ['uniprot_id', 'grch']:
        return name.lower()
    # CamelCase to pothole_case
    # if '_' not in name:
    # Don't split words if the name already uses underscores
    # e.g. UniProt_sequence, FoldX_value
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    name = name.lower()
    # Remove strange characters
    permitted = set(string.ascii_lowercase + string.digits + '_')
    name = ''.join(c for c in name if c in permitted)
    while name[0].isdigit():
        name = name[1:]
    while '__' in name:
        name = name.replace('__', '_')
    return name


def get_df_dtypes(df):
    """Return column dtypes for DataFrame `df`."""
    dtypes = {
        column: _get_column_dtype(column, dtype.char, df)
        for (column, dtype) in zip(df.columns, df.dtypes)
    }
    return dtypes


def _get_column_dtype(column, char, df):
    if char == 'l':
        return INTEGER
    elif char == 'd':
        return DOUBLE
    elif char == '?':
        return BOOLEAN
    elif char == 'O':
        max_len = df[column].str.len().max()
        if max_len <= 32:
            return VARCHAR_SHORT
        elif max_len <= 255:
            return VARCHAR_MEDIUM
        else:
            return TEXT


def get_file_dtypes(file, nrows=int(1e5), nchunks=100, **vargs):
    """Return column dtypes for file `file`.

    Parameters
    ----------
    file : str
        Full path to file.
    nrows : int | None
    nchunks : int | None
    vargs : dict
        Values to pass to `pd.read_cvs`.

    Returns
    -------
    df : DataFrame
        Empty DataFrame with the correct dtypes.
    dtypes : dict
        A dictionary of dtypes for each column
    """
    logger.debug('get_file_dtypes({}, {}, {}, {})'.format(file, nrows, nchunks, vargs))

    # Parse params
    if 'error_bad_lines' not in vargs:
        vargs['error_bad_lines'] = False
    if 'low_memory' not in vargs:
        vargs['low_memory'] = False  # helps with mixed dtypes?
    if op.splitext(file)[-1] in ['.gz', '.bz2', '.xz']:
        raise Exception("Compressed files are not supported!")
    # Get number of lines in file
    system_command = "wc -l '{}'".format(file)
    stdout, stderr, returncode = run_command(system_command, host=STG_HOST)
    num_lines = int(stdout.strip().split(' ')[0])
    logger.debug("nrows: {}".format(nrows))
    logger.debug("num_lines: {}".format(num_lines))
    # Parse file
    if nrows == 0:
        # Create empty dataframe with the right columns
        df = pd.read_csv(file, nrows=0, **vargs)
        dtypes_final = get_df_dtypes(df)
    elif nrows >= num_lines or nchunks == 1:
        # Read all rows in one go
        df = pd.read_csv(file, **vargs)
        dtypes_final = get_df_dtypes(df)
    else:
        # Read file chunk-by-chunk
        logger.debug("Reading file in chunks...")
        nchunks_max = math.ceil(num_lines / nrows)
        if nchunks in [0, None]:
            nchunks = nchunks_max
        elif nchunks < nchunks_max:
            logger.debug("Reading only {} out of {} chunks...".format(nchunks, nchunks_max))
        else:
            assert (nrows * nchunks) >= num_lines and (nrows * nchunks_max) >= num_lines
            nchunks = nchunks_max
        #
        dtypes_list = []
        vargs['chunksize'] = nrows
        for i, df in enumerate(pd.read_csv(file, **vargs)):
            dtypes = get_df_dtypes(df)
            dtypes_list.append(dtypes)
            if i > nchunks:
                break
        dtypes_final = _combine_dtypes(dtypes_list)
    return df[0:0], dtypes_final


def _combine_dtypes(dtypes_list):
    if not dtypes_list or not len(dtypes_list):
        raise ValueError(dtypes_list)
    elif len(dtypes_list) == 1:
        return dtypes_list[0]
    elif len(dtypes_list) == 2 and dtypes_list[0] is None:
        return dtypes_list[1]
    else:
        columns = {
            key for keys in [
                dtypes_list[i].keys() for i in range(len(dtypes_list))]
            for key in keys
        }
        return {
            column: _get_overall_dtypes([
                dtypes_list[i][column]
                for i in range(len(dtypes_list))
                if column in dtypes_list[i]
            ])
            for column in columns
        }


def _get_overall_dtypes(values):
    """.

    .. note::

        We need to str(...) dtypes because no two VARCHARS, etc. are the same.
    """
    str_values = [str(v) for v in values]
    if str(TEXT) in str_values:
        return TEXT
    elif str(VARCHAR_MEDIUM) in str_values:
        return VARCHAR_MEDIUM
    elif str(VARCHAR_SHORT) in str_values:
        return VARCHAR_SHORT
    elif str(DOUBLE) in str_values:
        return DOUBLE
    elif str(INTEGER) in str_values:
        return INTEGER
    else:
        print(values)
        raise ValueError("'values' contains unsupported dtypes:\n{}".format(values))
