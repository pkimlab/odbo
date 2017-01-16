import logging
import os
import os.path as op
import shlex
import string
import subprocess

import pandas as pd

from kmtools.db_tools import parse_connection_string
from kmtools.system_tools import iter_stdout, start_subprocess

logger = logging.getLogger(__name__)


class _Table:
    pass


# === MySQL / MariaDB ===

class MySQLTable(_Table):

    def __init__(self, name, df, dtypes, tempfile, connection_string, engine, datadir):
        self.name = name
        self.df = df
        self.dtypes = dtypes
        self.tempfile = tempfile
        self.connection_string = connection_string
        self.engine = engine
        self.datadir = datadir

    def get_indexes(self):
        db_params = parse_connection_string(self.connection_string)
        sql_query = """\
SELECT DISTINCT INDEX_NAME FROM information_schema.statistics
WHERE table_schema = '{db_schema}'
AND table_name = '{tablename}';
""".format(db_schema=db_params['db_schema'], tablename=self.name)
        existing_indexes = set(pd.read_sql_query(sql_query, self.engine)['INDEX_NAME'])
        return existing_indexes

    def create_indexes(self, index_commands):
        existing_indexes = self.get_indexes()
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
                    tablename=self.name,
                    columns=", ".join(columns))
            )
            self.engine.execute(sql_command)

    def add_idx_column(self, column_name='idx', auto_increment=1):
        sql_command = """\
ALTER TABLE {table_name}
AUTO_INCREMENT = {auto_increment},
ADD COLUMN {column_name} BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST
""".format(table_name=self.name, column_name=column_name, auto_increment=auto_increment)
        self.engine.execute(sql_command)
        max_id = pd.read_sql_query(
            "SELECT MAX({column_name}) from {table_name}".format(
                table_name=self.name, column_name=column_name),
            self.engine,
        )
        return int(max_id.values)

    def compress(self):
        db_params = parse_connection_string(self.connection_string)
        db_file = op.abspath(op.join(self.datadir, db_params['db_schema'], self.name + '.MYD'))
        index_file = op.abspath(op.join(self.datadir, db_params['db_schema'], self.name + '.MYI'))
        file_size_before = op.getsize(db_file) / (1024 ** 2)
        # Flush table
        self.engine.execute('flush tables;')
        # Compress table
        system_command = "myisampack --no-defaults '{}'".format(index_file)
        logger.debug("system_command: '{}'".format(system_command))
        p1 = subprocess.run(
            shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True)
        if p1.stdout.strip():
            logger.debug(p1.stdout.strip())
        if p1.stderr.strip():
            logger.error(p1.stderr.strip())
        if p1.returncode:
            raise Exception("Failed to compress table (returncode = {})".format(p1.returncode))
        # Recreate indexes
        system_command = "myisamchk -rq '{}'".format(index_file)
        logger.debug("system_command: '{}'".format(system_command))
        p2 = subprocess.run(
            shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True)
        if p2.stdout.strip():
            logger.debug(p2.stdout.strip())
        if p2.stderr.strip():
            logger.error(p2.stderr.strip())
        if p2.returncode:
            raise Exception("Failed to recreate indexes (returncode = {})".format(p2.returncode))
        file_size_after = op.getsize(db_file) / (1024 ** 2)
        logger.info(
            "File size before: {:,.2f} MB".format(file_size_before))
        logger.info(
            "File size after: {:,.2f} MB".format(file_size_after))
        logger.info(
            "File size savings: {:,.2f} MB ({:.2f} %)"
            .format(file_size_after, file_size_after / file_size_before * 100))

    def compress_all(self):
        """Compress all MyISAM files in a given directory."""
        data_files = [
            op.abspath(op.join(self.datadir, self.name, f))
            for f in os.listdir(op.join(self.datadir, self.name))
            if op.splitext(f)[-1] == '.MYI'
        ]
        data_files_str = " ".join("'{}'".format(op.abspath(f)) for f in data_files)
        # Compress files
        system_command = "myisampack --no-defaults '{}'".format(data_files_str)
        # allowed_returncodes=[0, 2]
        p = start_subprocess(system_command)
        for line in iter_stdout(p):
            logger.debug(line)
        # Re-create index
        system_command = "myisamchk -rq '{}'".format(data_files_str)
        p = start_subprocess(system_command)
        for line in iter_stdout(p):
            logger.debug(line)
