import os
import os.path as op
import argparse
import contextlib
import logging
import shlex
import subprocess
import gzip
import csv
import re

from retrying import retry
from paramiko import SSHClient
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import INTEGER, DOUBLE, VARCHAR, MEDIUMTEXT

from common import dat

logger = logging.getLogger(__name__)

logging.getLogger("paramiko").setLevel(logging.WARNING)


# %%
INTEGER = INTEGER()
DOUBLE = DOUBLE()
VARCHAR = VARCHAR(255)
TEXT = MEDIUMTEXT()

STG_HOST = '192.168.6.8'
REMOVED_EXTENSIONS = ['.gz', '.tsv', '.csv', '.txt', '.vcf']


# %%
class MySSHClient:

    _ssh_clients = {}

    def __init__(self, ssh_host):
        self.ssh_host = ssh_host
        if ssh_host not in MySSHClient._ssh_clients:
            logger.debug("Initializing SSH client: '{}'".format(ssh_host))
            client = SSHClient()
            client.load_system_host_keys()
            client.connect(ssh_host)
            MySSHClient._ssh_clients[ssh_host] = client
        self.client = MySSHClient._ssh_clients[ssh_host]

    def exec_command(self, command, **varargs):
        return self.client.exec_command(command, **varargs)


# %% Library functions
def check_exception(exc, valid_exc):
    logger.error('The following exception occured:\n{}'.format(exc))
    to_retry = isinstance(exc, valid_exc)
    if to_retry:
        logger.error('Retrying...')
    return to_retry


def retry_database(fn):
    """Decorator to keep probing the database untill you succeed.
    """
    r = retry(
        retry_on_exception=lambda exc:
            check_exception(exc, valid_exc=sa.exc.OperationalError),
        wait_exponential_multiplier=1000,
        wait_exponential_max=60000,
        stop_max_attempt_number=7)
    return r(fn)


def run_command(system_command, host=None):
    logger.debug(system_command)
    if host is not None:
        logger.debug("Running on host: '{}'".format(host))
        client = MySSHClient(host)
        stdin, stdout, stderr = client.exec_command(system_command)
        result = stdout.read().decode() + stderr.read().decode()
    else:
        logger.debug("Running locally")
        sp = subprocess.run(
            shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        result = sp.stdout + sp.stderr
        logger.debug('returncode: {}'.format(sp.returncode))
    logger.debug("result: {}".format(result))
    logger.debug("done printing result!")


# %%
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', type=str)
    parser.add_argument(
        '--db', dest='connection_string', required=True,
        help='If present, a sqlalchemy connection string to use to directly execute generated SQL '
             'on a database.'
    )
    parser.add_argument('--sep', type=str, default='\t')
    parser.add_argument('--nrows', type=int,
                        help='Number of rows to read when figuring out dtypes.')
    parser.add_argument('--skiprows', type=int, default=0,
                        help='Number of rows other than the header row.')
    parser.add_argument('--na_values', type=str, default=['', '.'])
    parser.add_argument('--debug', action='store_true', default=False)
    args = parser.parse_args()
    return args


def configure_logger(debug=False):
    import logging.config
    LOGGING_CONFIGS = {
        'version': 1,
        'disable_existing_loggers': False,  # this fixes the problem

        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            },
            'clean': {
                'format': '%(message)s',
            },
        },
        'handlers': {
            'default': {
                'level': 'DEBUG' if debug else 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'clean',
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': 'DEBUG' if debug else 'INFO',
                'propagate': True
            }
        }
    }
    logging.config.dictConfig(LOGGING_CONFIGS)


# %%
def sniff_file(path, snifflimit=10000):
    mode = 'rt'
    if path.endswith('.gz'):
        fh = gzip.open(path, mode)
    else:
        fh = open(path, mode)
    s = csv.Sniffer().sniff(fh.read(snifflimit))
    return s


def get_column_dtype(column, char, df):
    """
    """
    if char == 'l':
        return INTEGER
    elif char == 'd':
        return DOUBLE
    elif char == 'O':
        max_len = df[column].str.len().max()
        if max_len <= 255:
            return VARCHAR
        else:
            return TEXT


def get_column_dtypes(df):
    dtypes = {
        column: get_column_dtype(column, dtype.char, df)
        for (column, dtype) in zip(df.columns, df.dtypes)
    }
    return dtypes


def get_overall_dtypes(values):
    """
    .. note::

        We need to str(...) dtypes because no two VARCHARS, etc. are the same.
    """
    str_values = [str(v) for v in values]
    if str(TEXT) in str_values:
        return TEXT
    elif str(VARCHAR) in str_values:
        return VARCHAR
    elif str(DOUBLE) in str_values:
        return DOUBLE
    elif str(INTEGER) in str_values:
        return INTEGER
    else:
        print(v)
        raise ValueError("'values' contains unsupported dtypes:\n{}".format(values))


def add_dtypes(dtypes):
    if not dtypes or not len(dtypes):
        raise ValueError(dtypes)
    elif len(dtypes) == 1:
        return dtypes[0]
    elif len(dtypes) == 2 and dtypes[0] is None:
        return dtypes[1]
    else:
        columns = {
            key for keys in [dtypes[i].keys() for i in range(len(dtypes))] for key in keys
        }
        return {
            column: get_overall_dtypes(
                [dtypes[i][column] for i in range(len(dtypes)) if column in dtypes[i]])
            for column in columns
        }


def get_dtypes_for_file(path, snufflimit=int(1e8), **vargs):
    """
    Parameters
    ----------
    vargs : dict
        Values to pass to `pd.read_cvs`.
    """
    logger.debug('get_dtypes_for_file({}, {}, {})'.format(path, snufflimit, vargs))

    # Options
    if 'error_bad_lines' not in vargs:
        vargs['error_bad_lines'] = False
    if 'low_memory' not in vargs:
        vargs['low_memory'] = False  # helps with mixed dtypes?
    # Chunks
    fsize = os.stat(path).st_size
    size_limit = 500 if not path.endswith('.gz') else 100  # in MB
    do_parts = fsize // 1024 // 1024 // size_limit
    # Read
    if do_parts:
        # TODO: Optimize chunk size
        num_chunks = 100
        vargs['chunksize'] = snufflimit // num_chunks
        logger.debug(
            "Reading file in chunks of size: {:,.0f} ({} chunks)"
            .format(vargs['chunksize'], num_chunks))
        dtypes_list = []
        for i, df in enumerate(pd.read_csv(path, **vargs)):
            dtypes = get_column_dtypes(df)
            dtypes_list.append(dtypes)
            if i > num_chunks:
                break
        dtypes_final = add_dtypes(dtypes_list)
    else:
        df = pd.read_csv(path, **vargs)
        dtypes_final = get_column_dtypes(df)
    return df[0:0], dtypes_final


# %% Main functions
@contextlib.contextmanager
def decompress(filepath, keep_uncompressed=False):
    filepath_tsv, ext = op.splitext(filepath)
    # File not compressed
    if not filepath.endswith('.gz'):
        logger.debug("File '{}' is not compressed...".format(filepath_tsv))
        yield filepath_tsv
        return
    # File compressed
    try:
        logger.debug("Uncompressing file...".format(filepath))
        system_command = "7z x '{}' -o'{}' -y".format(filepath, op.dirname(filepath))
        run_command(system_command, host=STG_HOST)
        yield filepath_tsv
    except Exception as e:
        logger.error('{}: {}'.format(type(e), e))
        raise e
    finally:
        if not keep_uncompressed:
            logger.debug("Removing uncompressed file...".format(filepath_tsv))
            system_command = "rm '{}'".format(filepath_tsv)
            run_command(system_command)


def get_table_name(path):
    name = op.basename(path)
    while True:
        basename, ext = op.splitext(name)
        if ext in REMOVED_EXTENSIONS:
            name = basename
            continue
        else:
            break
    return name


@retry_database
def create_db_table(name, df, dtypes, connection_string):
    engine = sa.create_engine(connection_string)
    df[0:0].to_sql(name, engine, dtype=dtypes, index=False, if_exists='replace')
    # engine.execute("TRUNCATE `{}`".format(name))


def convert_na_values(filepath_tsv, na_values):
    if na_values is None:
        return
    if not isinstance(na_values, (list, tuple)):
        na_values = [na_values]

    system_command_head = "sed -i "
    system_command_body = ""
    system_command_tail = " {}".format(filepath_tsv)
    for na_value in na_values:
        if na_value and na_value in "$.*[\\]^'\"":
            na_value = '\\' + na_value
        system_command_body += (
            r"-e 's/\t{0}\t/\t\\N\t/g' "
            r"-e 's/\t{0}\t/\t\\N\t/g' "
            r"-e 's/^{0}\t/\\N\t/g' "
            r"-e 's/\t{0}$/\t\\N/g' "
            .format(na_value)
        )
    if system_command_body:
        system_command = ''.join([system_command_head, system_command_body, system_command_tail])
        run_command(system_command)  # NB: sed is CPU-bound, no need to do remotely


def vcf2tsv(filepath_vcf):
    system_command = "sed -i -e '/^##/d' -e 's/^#//' {}".format(filepath_vcf)
    run_command(system_command)  # NB: sed is CPU-bound, no need to do remotely


def load_file_to_database(path_tsv, table_name, connection_string, skiprows=1):
    logger.debug("Loading data into MySQL table: '{}'...".format(table_name))
    # Database options
    db_params = {}
    (db_params['db_type'], db_params['username'], db_params['password'],
     db_params['host_ip'], db_params['host_port'], db_params['db_name']) = (
        re.match('(\w*)://(\w*):(\w*)@([0-9\.]*):([0-9]*)\/(\w*)', connection_string)
        .groups()
    )
    if 'password' in db_params:
        db_params['password'] = (
            '-p {}'.format(db_params['password']) if db_params['password'] else ''
        )
    # Run
    system_command = (
        "mysql --local-infile -h {host_ip} -P {host_port} -u {username} {password} {db_name} -e "
        "\"load data local infile '{path_tsv}' into table `{table_name}` ignore {skiprows} lines; "
        "show warnings;\""
        .format(path_tsv=path_tsv, table_name=table_name, skiprows=skiprows, **db_params)
    )
    run_command(system_command)


# %%
MYSQL_TOCSV_OPTS = dict(
    sep='\t',
    na_rep='\\N',
    index=False,
    header=True,
    escapechar='\\',
)


# %% Main
def main(path, connection_string, table_name=None, storage_host=None, **vargs):
    """
    Parameters
    ----------
    vargs : dict
        Options to pass to `pd.read_csv`.
    """
    # Storage server
    if storage_host is not None:
        global STG_HOST
        STG_HOST = storage_host
    # Default parameters
    vargs['sep'] = vargs.get('sep', '\t')
    vargs['na_values'] = vargs.get('na_values', ['', '\\N', '.', 'na'])
    table_name = table_name if table_name else get_table_name(path)
    # Create table with proper dtypes
    with decompress(path) as path_tsv:
        # Edit file
        if path_tsv.endswith('.vcf'):
            vcf2tsv(path_tsv)
        convert_na_values(path_tsv, vargs['na_values'])
        # Get column types and create a dataframe
        df, dtypes = get_dtypes_for_file(path_tsv, **vargs)
        logger.debug('df: {}'.format(df))
        logger.debug('dtypes: {}'.format(dtypes))
        create_db_table(table_name, df, dtypes, connection_string)
        # Upload file to database
        mysql_skiprows = vargs.get('skiprows', 0) + 1
        load_file_to_database(path_tsv, table_name, connection_string, mysql_skiprows)


# %%
if __name__ == '__main__':
    args = parse_args()
    dat.configure_logger(logger, formatter='[%(levelname)s]: %(message)s')
    #
    print(args)
    path = op.abspath(args.file)
    main(path, args.connection_string,
         sep=args.sep, nrows=args.nrows, skiprows=args.skiprows, na_values=args.na_values)
