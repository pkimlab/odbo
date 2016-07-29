import os
import os.path as op
import logging
import string
from collections import Counter
import csv
import pandas as pd
import sqlalchemy as sa
from kmtools.db_tools import parse_connection_string, make_connection_string
from .utils import (
    run_command, retry_database, get_tablename, get_df_dtypes, get_file_dtypes, format_columns
)
from ._format_file_bash import decompress

logger = logging.getLogger(__name__)

MYSQL_CSV_OPTS = dict(
    sep='\t',
    na_rep='\\N',
    index=False,
    header=True,
    quoting=0,
    # escapechar='\\',  # this screws up nulls (\N) because it tries to escape them... :(
)


class Table:

    def __init__(self, name, df, dtypes, tempfile):
        self.name = name
        self.df = df
        self.dtypes = dtypes
        self.tempfile = tempfile


class MySQL:
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
        try:
            self.db_schema = self._get_db_schema()
        except sa.exc.OperationalError:
            db_params = parse_connection_string(connection_string)
            _schema = db_params['db_schema']
            db_params['db_schema'] = ''
            logger.debug("db_params: {}".format(db_params))
            _connection_string = make_connection_string(**db_params)
            logger.debug("_connection_string: {}".format(_connection_string))
            _engine = sa.create_engine(_connection_string, echo=echo)
            _engine.execute('CREATE DATABASE {}'.format(_schema))
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
                'ALTER TABLE {tablename} ENGINE={db_engine};'  # ROW_FORMAT = FIXED
                .format(tablename=tablename, db_engine=self.db_engine))
        # Set compression
        if self.use_compression and self.db_engine == 'InnoDB':
            self.engine.execute(
                'ALTER TABLE {tablename} ROW_FORMAT=COMPRESSED;'.format(tablename=tablename))

    def load_file_to_database(
            self, tsv_filepath, tablename, sep, quotechar='"', quoting=csv.QUOTE_MINIMAL,
            skiprows=1):
        logger.debug("Loading data into MySQL table: '{}'...".format(tablename))

        # Database options
        db_params = parse_connection_string(self.connection_string)

        # Run
        if db_params['db_socket']:
            header = "--socket={db_socket}".format(**db_params)
        elif db_params['db_password']:
            header = "-h {db_url} -P {db_port} -p{db_password}".format(**db_params)
        else:
            header = "-h {db_url} -P {db_port}".format(**db_params)

        if quotechar == '"':
            quotechar = '\\"'
        if (quoting is None or
                (quoting == csv.QUOTE_MINIMAL or quoting == 0) or
                (quoting == csv.QUOTE_NONNUMERIC or quoting == 2)):
            quoting = """optionally enclosed by '{}'""".format(quotechar)
        elif (quoting == csv.QUOTE_ALL or quoting == 3):
            quoting = """enclosed by '{}'""".format(quotechar)
        else:
            quoting = ""
        system_command = """\
mysql --local-infile {header} -u {db_username} {db_schema} -e \
"load data local infile '{tsv_filepath}' into table `{tablename}` \
fields terminated by {sep} {quoting} ignore {skiprows} lines; \
 show warnings;" \
""".format(header=header, tsv_filepath=tsv_filepath, tablename=tablename, skiprows=skiprows,
           sep=repr(sep), quoting=quoting, **db_params)
        run_command(system_command)

    def add_idx_column(self, table_name, column_name='idx', auto_increment=1):
        sql_command = """\
ALTER TABLE {table_name}
AUTO_INCREMENT = {auto_increment},
ADD COLUMN {column_name} BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST
""".format(table_name=table_name, column_name=column_name, auto_increment=auto_increment)
        self.engine.execute(sql_command)
        max_id = pd.read_sql_query(
            "SELECT MAX({column_name}) from {table_name}".format(
                table_name=table_name, column_name=column_name),
            self.engine,
        )
        return int(max_id.values)

    def get_indexes(self, tablename):
        db_params = parse_connection_string(self.connection_string)
        sql_query = """\
SELECT DISTINCT INDEX_NAME FROM information_schema.statistics
WHERE table_schema = '{db_schema}'
AND table_name = '{tablename}';
""".format(db_schema=db_params['db_schema'], tablename=tablename)
        existing_indexes = set(pd.read_sql_query(sql_query, self.engine)['INDEX_NAME'])
        return existing_indexes

    def create_indexes(self, tablename, index_commands):
        existing_indexes = self.get_indexes(tablename)
        valid_indexes = [c for c in string.ascii_uppercase if c not in existing_indexes]
        for index_name, index_command in zip(valid_indexes, index_commands):
            columns, unique = index_command
            if not isinstance(columns, (list, tuple)):
                columns = [columns]
            sql_command = (
                "create {unique} index {index_name} on {tablename} ({columns});"
                .format(
                    unique='unique' if unique else '',
                    index_name=index_name,
                    tablename=tablename,
                    columns=", ".join(columns))
            )
            self.engine.execute(sql_command)

    def import_file(
            self, file, tablename=None, dtypes=None, extra_dtypes=None,
            extra_substitutions=None, use_tmp=False, keep_tmp=False, **csv_opts):
        """Load file `file` into database table `tablename`.

        Parameters
        ----------
        additional_substitutions : list of tuples
            Additional substitutions to perform on the file before loading to database.
        vargs : dict
            Options to pass to `pd.read_csv`.
        """
        if extra_substitutions is None:
            extra_substitutions = []

        # Default parameters
        csv_opts['sep'] = csv_opts.get('sep', '\t')
        csv_opts['na_values'] = csv_opts.get('na_values', ['', '\\N', '.', 'na'])
        if isinstance(csv_opts['na_values'], str):
            csv_opts['na_values'] = [csv_opts['na_values']]
        csv_opts['quotechar'] = csv_opts.get('quotechar', '"')
        csv_opts['quoting'] = csv_opts.get('quoting', csv.QUOTE_MINIMAL)

        tablename = tablename if tablename else get_tablename(file)

        if '.vcf' in op.basename(file).lower():
            extra_substitutions.append('/^##/d')
        outfile = decompress(
            infile=file, sep=csv_opts['sep'], na_values=csv_opts['na_values'],
            extra_substitutions=extra_substitutions, use_tmp=use_tmp)

        # Get column types and create a dataframe
        if dtypes is None:
            df, dtypes = get_file_dtypes(outfile, **csv_opts)
            df.columns = format_columns(df.columns)
            dtypes = {format_columns(k): v for k, v in dtypes.items()}
            if extra_dtypes:
                if set(extra_dtypes.keys()) - set(dtypes.keys()):
                    logger.warning(
                        "The following dtypes were not applied: ({})"
                        .format(set(extra_dtypes.keys()) - set(dtypes.keys())))
                dtypes = {**dtypes, **extra_dtypes}
        else:
            df, _ = get_file_dtypes(outfile, nrows=0, **csv_opts)
            df.columns = format_columns(df.columns)

        self.create_db_table(tablename, df, dtypes)

        # Upload file to database
        db_skiprows = csv_opts.get('skiprows', 0) + 1
        self.load_file_to_database(
            outfile, tablename, csv_opts['sep'], csv_opts['quotechar'], csv_opts['quoting'],
            db_skiprows)

        if not keep_tmp:
            try:
                os.remove(outfile)
            except FileNotFoundError:
                pass
        return Table(name=tablename, df=df, dtypes=dtypes, tempfile=outfile)

    def import_df(
            self, df, tablename=None, dtypes=None, extra_dtypes=None, use_temp_file=True,
            if_exists='replace', force=True):
        """Load dataframe `df` into database table `tablename`.

        Parameters
        ----------
        df : DataFrame
            Need this to guess the columns types.
        tablename : str
            Name of the table to create in the database.
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
        if extra_dtypes:
            dtypes = {**dtypes, **extra_dtypes}
        self.create_db_table(tablename, df, dtypes, empty=use_temp_file, if_exists=if_exists)
        # If `use_temp_file`, save a .tsv file and load it into the database
        if use_temp_file:
            tsv_file = op.abspath(op.join(self.shared_folder, tablename + '.tsv'))
            if op.isfile(tsv_file) and not force:
                logger.info("tempfile already exists: {}".format(tsv_file))
            else:
                df.to_csv(tsv_file, **MYSQL_CSV_OPTS)
            self.load_file_to_database(tsv_file, tablename, '\t', skiprows=1)
        else:
            tsv_file = None
        return Table(name=tablename, df=df[0:0], dtypes=dtypes, tempfile=tsv_file)
