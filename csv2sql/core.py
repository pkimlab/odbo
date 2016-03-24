import os
import os.path as op
import string
import contextlib
import logging
import shlex
import subprocess
import gzip
import csv
import re
import time
from collections import Counter

from retrying import retry
from paramiko import SSHClient
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import INTEGER, DOUBLE, BOOLEAN, VARCHAR, MEDIUMTEXT

logger = logging.getLogger(__name__)

logging.getLogger("paramiko").setLevel(logging.WARNING)


# %%
INTEGER = INTEGER()
DOUBLE = DOUBLE()
VARCHAR = VARCHAR(255)
TEXT = MEDIUMTEXT()
BOOLEAN = BOOLEAN()

#: IP address of the NFS server
#: (so that you don't have to uncompress files over the network)
STG_HOST = None

#: Extensions that get stripped when creating a database table from a text file
REMOVED_EXTENSIONS = ['.gz', '.tsv', '.csv', '.txt', '.vcf']


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


def rety_subprocess(fn):
    r = retry(
        retry_on_exception=lambda exc:
            check_exception(exc, valid_exc=MySubprocessError),
        wait_exponential_multiplier=1000,
        wait_exponential_max=60000,
        stop_max_attempt_number=7)
    return r(fn)


# %% Run system command either locally or over ssh
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


class MySubprocessError(subprocess.SubprocessError):

    def __init__(self, command, host, output, returncode):
        self.command = command
        self.host = host
        self.output = output
        self.returncode = returncode


@rety_subprocess
def run_command(system_command, host=None):
    """
    """
    logger.debug(system_command)
    if host is not None:
        logger.debug("Running on host: '{}'".format(host))
        client = MySSHClient(host)
        stdin, stdout, stderr = client.exec_command(system_command)
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
    elif char == '?':
        return BOOLEAN
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
def uncompressed(filepath, keep_uncompressed=False, stg_host=None, force=True):
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
        if ext == '.gz':
            system_command = "gzip -dkf '{}'".format(filepath)
        elif ext == '.bz2':
            system_command = "bzip2 -dkf '{}'".format(filepath)
        run_command(system_command, host=stg_host)
        n_tries = 0
        while n_tries < 10:
            if op.isfile(filepath_tsv):
                break
            else:
                print("FML")
                time.sleep(n_tries * 10)
                n_tries += 1
        assert op.isfile(filepath_tsv)
        yield filepath_tsv
    except Exception as e:
        logger.error('{}: {}'.format(type(e), e))
        raise e
    finally:
        if not keep_uncompressed:
            logger.debug("Removing uncompressed file '{}'...".format(filepath_tsv))
            system_command = "rm -f '{}'".format(filepath_tsv)
            run_command(system_command)
            assert not op.isfile(filepath_tsv)


def get_tablename(path):
    name = op.basename(path)
    while True:
        basename, ext = op.splitext(name)
        if ext in REMOVED_EXTENSIONS:
            name = basename
            continue
        else:
            break
    return name


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


# %% To SQL
class _ToSQL:
    """
    Load and save data from a database using intermediary csv files
    (much faster than pandas ``df.to_sql(...)``.
    """

    def __init__(self, connection_string, shared_folder, storage_host, echo=False):
        global STG_HOST
        self.connection_string = connection_string
        self.shared_folder = op.abspath(shared_folder)
        os.makedirs(self.shared_folder, exist_ok=True)
        self.storage_host = storage_host
        self.engine = sa.create_engine(connection_string, echo=echo)
        self.db_schema = self._get_db_schema()
        STG_HOST = self.storage_host

    def _get_db_schema(self):
        raise NotImplementedError

    def import_table(self, df, tablename, index_commands):
        raise NotImplementedError

    def export_table(self, tablename):
        raise NotImplementedError


# %% MySQL
MYSQL_CSV_OPTS = dict(
    sep='\t',
    na_rep='\\N',
    index=False,
    header=True,
    quoting=0,
    # escapechar='\\',  # this screws up nulls (\N) because it tries to escape them... :(
)


class _ToMySQL(_ToSQL):

    def _get_db_schema(self):
        return set(pd.read_sql_query('show databases;', self.engine)['Database'])

    @retry_database
    def create_db_table(self, tablename, df, dtypes, empty=True, if_exists='replace'):
        """Create a table `tablename` in the database.

        If `empty` == True, do not load any data. Otherwise load the entire `df` into the created
        table.
        """
        if empty:
            df = df[:0]
        df.to_sql(tablename, self.engine, dtype=dtypes, index=False, if_exists=if_exists)

    @staticmethod
    def parse_connection_string(connection_string):
        # Specific to MySQL because of password
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
        return db_params

    def load_file_to_database(self, tsv_filepath, tablename, skiprows=1):
        logger.debug("Loading data into MySQL table: '{}'...".format(tablename))
        # Database options
        db_params = self.parse_connection_string(self.connection_string)
        # Run
        system_command = """\
mysql --local-infile -h {host_ip} -P {host_port} -u {username} {password} {db_name} -e \
"load data local infile '{tsv_filepath}' into table `{tablename}` ignore {skiprows} lines; \
 show warnings;" \
""".format(tsv_filepath=tsv_filepath, tablename=tablename, skiprows=skiprows, **db_params)
        run_command(system_command)

    def create_indices(self, tablename, index_commands):
        for index_name, index_command in zip(string.ascii_letters, index_commands):
            columns, unique = index_command
            sql_command = (
                "create {unique} index {index_name} on {tablename} ({columns});"
                .format(
                    unique='unique' if unique else '',
                    index_name=index_name,
                    tablename=tablename,
                    columns=", ".join(columns))
            )
            self.engine.execute(sql_command)


class FileToMySQL(_ToMySQL):

    def import_table(self, filename, tablename=None, index_commands=None, **vargs):
        """Load file `filename` into database table `tablename`.

        Parameters
        ----------
        vargs : dict
            Options to pass to `pd.read_csv`.
        """
        # Default parameters
        vargs['sep'] = vargs.get('sep', '\t')
        vargs['na_values'] = vargs.get('na_values', ['', '\\N', '.', 'na'])
        tablename = tablename if tablename else get_tablename(filename)
        # Create table with proper dtypes
        filepath = op.abspath(op.join(self.shared_folder, filename))
        with uncompressed(filepath) as tsv_filepath:
            # Edit file
            if tsv_filepath.endswith('.vcf'):
                vcf2tsv(tsv_filepath)
            convert_na_values(tsv_filepath, vargs['na_values'])
            # Get column types and create a dataframe
            df, dtypes = get_dtypes_for_file(tsv_filepath, **vargs)
            logger.debug('df: {}'.format(df))
            logger.debug('dtypes: {}'.format(dtypes))
            self.create_db_table(tablename, df, dtypes)
            # Upload file to database
            db_skiprows = vargs.get('skiprows', 0) + 1
            self.load_file_to_database(tsv_filepath, tablename, db_skiprows)
        if index_commands:
            self.create_indices(tablename, index_commands)


class DataFrameToMySQL(_ToMySQL):

    def import_table(
            self, df, tablename, index_commands=None, use_temp_file=True, if_exists='replace',
            force=True):
        """Load dataframe `df` into database table `tablename`.

        Parameters
        ----------
        df : DataFrame
            Need this to guess the columns types.
        tablename : str
            Name of the table to create in the database.
        index_commands : list of tuples
            List of tuples describing the indexes that should be created. E.g.
            `[(['a', 'b'], True), (['b', 'a'], False)]`
        use_temp_file : bool
            Whether to save data to a .tsv file first, or import directly.
        if_exists : str
            What to do if the specified table already exists in the database.
        """
        # Make sure there are no duplicate columns silently screwing everything up
        column_counts = Counter(df.columns)
        duplicate_columns = [x for x in column_counts.items() if x[1] > 1]
        if duplicate_columns:
            raise Exception("The following columns have duplicates: {}".format(duplicate_columns))
        # Sniff out column dtypes and create a db table
        dtypes = get_column_dtypes(df)
        self.create_db_table(tablename, df, dtypes, empty=use_temp_file, if_exists=if_exists)
        # If `use_temp_file`, save a .tsv file and load it into the database
        if use_temp_file:
            bz2_filename = op.join(self.shared_folder, tablename + '.tsv.bz2')
            if op.isfile(bz2_filename) and not force:
                logger.info("bzip2 file already exists: {}".format(bz2_filename))
            else:
                df.to_csv(bz2_filename, compression='bz2', **MYSQL_CSV_OPTS)
            with uncompressed(bz2_filename, stg_host=self.storage_host, force=force) as tsv_filename:
                self.load_file_to_database(tsv_filename, tablename, 1)
        if index_commands:
            self.create_indices(tablename, index_commands)
