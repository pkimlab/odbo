import os
import os.path as op
import logging
import string
from collections import Counter
import pandas as pd
import sqlalchemy as sa
from ._helper import parse_connection_string, decompress, run_command, retry_database
from ._df_helper import (
    get_tablename, get_df_dtypes, get_file_dtypes, convert_na_values, vcf2tsv)

logger = logging.getLogger(__name__)

MYSQL_CSV_OPTS = dict(
    sep='\t',
    na_rep='\\N',
    index=False,
    header=True,
    quoting=0,
    # escapechar='\\',  # this screws up nulls (\N) because it tries to escape them... :(
)


class _ToMySQL:
    """Load and save data from a database using intermediary csv files.

    Much faster than pandas ``df.to_sql(...)``.
    """

    _default_db_engine = 'InnoDB'

    def __init__(
            self, connection_string, shared_folder, storage_host, echo=False, db_engine=None,
            use_compression=False):
        global STG_HOST
        self.connection_string = connection_string
        self.shared_folder = op.abspath(shared_folder)
        os.makedirs(self.shared_folder, exist_ok=True)
        self.storage_host = storage_host
        self.db_engine = db_engine if db_engine is not None else self._default_db_engine
        self.use_compression = use_compression
        #
        self.engine = sa.create_engine(connection_string, echo=echo)
        self.db_schema = self._get_db_schema()
        STG_HOST = self.storage_host

    def _get_db_schema(self):
        return set(pd.read_sql_query('show databases;', self.engine)['Database'])

    @retry_database
    def create_db_table(
            self, tablename, df, dtypes, empty=True, if_exists='replace'):
        """Create a table `tablename` in the database.

        If `empty` == True, do not load any data. Otherwise,
        load the entire `df` into the created table.
        """
        if empty:
            df = df[:0]
        df.to_sql(tablename, self.engine, dtype=dtypes, index=False, if_exists=if_exists)
        # Change storage engine
        if self.db_engine != self._default_db_engine:
            self.engine.execute(
                'ALTER TABLE {tablename} ENGINE={db_engine};'
                .format(tablename=tablename, db_engine=self.db_engine))
        # Set compression
        if self.use_compression and self.db_engine == 'InnoDB':
            self.engine.execute(
                'ALTER TABLE {tablename} ROW_FORMAT=COMPRESSED;'.format(tablename=tablename))

    def load_file_to_database(self, tsv_filepath, tablename, skiprows=1):
        logger.debug("Loading data into MySQL table: '{}'...".format(tablename))
        # Database options
        db_params = parse_connection_string(self.connection_string)
        if 'password' in db_params:
            db_params['password'] = (
                '-p {}'.format(db_params['password']) if db_params['password'] else ''
            )
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
        with decompress(filepath) as tsv_filepath:
            # Edit file
            if tsv_filepath.endswith('.vcf'):
                vcf2tsv(tsv_filepath)
            convert_na_values(tsv_filepath, vargs['na_values'])
            # Get column types and create a dataframe
            df, dtypes = get_file_dtypes(tsv_filepath, **vargs)
            logger.debug('df: {}'.format(df))
            logger.debug('dtypes: {}'.format(dtypes))
            self.create_db_table(tablename, df, dtypes)
            # Upload file to database
            db_skiprows = vargs.get('skiprows', 0) + 1
            self.load_file_to_database(tsv_filepath, tablename, db_skiprows)
        if index_commands:
            self.create_indices(tablename, index_commands)
        if self.use_compression and self.db_engine == 'MyISAM':
            self.compress_myisam_table(tablename)


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
        dtypes = get_df_dtypes(df)
        self.create_db_table(tablename, df, dtypes, empty=use_temp_file, if_exists=if_exists)
        # If `use_temp_file`, save a .tsv file and load it into the database
        if use_temp_file:
            bz2_filename = op.join(self.shared_folder, tablename + '.tsv.bz2')
            if op.isfile(bz2_filename) and not force:
                logger.info("bzip2 file already exists: {}".format(bz2_filename))
            else:
                df.to_csv(bz2_filename, compression='bz2', **MYSQL_CSV_OPTS)
            with decompress(bz2_filename, stg_host=self.storage_host, force=force) \
                    as tsv_filename:
                self.load_file_to_database(tsv_filename, tablename, 1)
        if index_commands:
            self.create_indices(tablename, index_commands)
