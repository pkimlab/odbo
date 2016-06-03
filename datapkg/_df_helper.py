import os
import os.path as op
import string
import logging
import re
import pandas as pd
from sqlalchemy.dialects.mysql import INTEGER, DOUBLE, BOOLEAN, VARCHAR, MEDIUMTEXT
from ._helper import run_command

logger = logging.getLogger(__name__)

INTEGER = INTEGER()
DOUBLE = DOUBLE()
VARCHAR = VARCHAR(255)
TEXT = MEDIUMTEXT()
BOOLEAN = BOOLEAN()

#: Extensions that get stripped when creating a database table from a text file
REMOVED_EXTENSIONS = ['.gz', '.tsv', '.csv', '.txt', '.vcf']


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
    # CamelCase to pothole_case
    if '_' not in name:
        # Don't split words if the name already uses underscores
        # e.g. UniProt_sequence, FoldX_value
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    name = name.lower()
    # Remove strange characters
    permitted = set(string.ascii_lowercase + '_')
    name = ''.join(c for c in name if c in permitted)
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
        if max_len <= 255:
            return VARCHAR
        else:
            return TEXT


def get_file_dtypes(file, snufflimit=int(1e8), **vargs):
    """Return column dtypes for file `file`.

    Parameters
    ----------
    file : str
        Full path to file.
    snufflimit : int
        Number of lines to read before guessing dtypes.
    vargs : dict
        Values to pass to `pd.read_cvs`.
    """
    logger.debug('get_dtypes_for_file({}, {}, {})'.format(file, snufflimit, vargs))

    # Options
    if 'error_bad_lines' not in vargs:
        vargs['error_bad_lines'] = False
    if 'low_memory' not in vargs:
        vargs['low_memory'] = False  # helps with mixed dtypes?
    # Chunks
    fsize = os.stat(file).st_size
    size_limit = 500 if not file.endswith('.gz') else 100  # in MB
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
        for i, df in enumerate(pd.read_csv(file, **vargs)):
            dtypes = get_df_dtypes(df)
            dtypes_list.append(dtypes)
            if i > num_chunks:
                break
        dtypes_final = _add_dtypes(dtypes_list)
    else:
        df = pd.read_csv(file, **vargs)
        dtypes_final = get_df_dtypes(df)
    return df[0:0], dtypes_final


def _add_dtypes(dtypes):
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
            column: _get_overall_dtypes(
                [dtypes[i][column] for i in range(len(dtypes)) if column in dtypes[i]])
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
    elif str(VARCHAR) in str_values:
        return VARCHAR
    elif str(DOUBLE) in str_values:
        return DOUBLE
    elif str(INTEGER) in str_values:
        return INTEGER
    else:
        print(values)
        raise ValueError("'values' contains unsupported dtypes:\n{}".format(values))


def convert_na_values(file, na_values):
    if na_values is None or na_values == ['\\N']:
        return
    if not isinstance(na_values, (list, tuple)):
        na_values = [na_values]

    system_command_head = "sed -i "
    system_command_body = ""
    system_command_tail = " {}".format(file)
    for na_value in na_values:
        if na_value == '\\N':
            continue
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


def vcf2tsv(file):
    system_command = "sed -i -e '/^##/d' -e 's/^#//' {}".format(file)
    run_command(system_command)  # NB: sed is CPU-bound, no need to do remotely
