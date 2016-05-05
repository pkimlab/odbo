import os.path as op
import argparse
import logging
from .core import FileToMySQL


logger = logging.getLogger(__name__)


def _file2db(args):
    db = FileToMySQL(
        # os.environ['DATAPKG_CONNECTION_STR'] + '/protein_folding_energy'
        connection_string=args.connection_string,
        # NOTEBOOK_NAME
        shared_folder=op.dirname(args.file),
        # os.environ['STG_SERVER_IP']
        storage_host=args.storage_host,
        echo=args.debug,
    )
    tablename = ''.join(
        c for c in (
            op.splitext(op.basename(args.file))[0]
            .lower()
            .replace('-', '_')
            .replace('.', '_')
        ) if c.isalnum() or c == '_')
    logger.info(tablename)
    db.import_table(
        filename=op.abspath(args.file),
        tablename=tablename,
        index_commands=None,
        # **vargs
        sep=args.sep, nrows=args.nrows, skiprows=args.skiprows, na_values=args.na_values,
    )


def configure_file2db_parser(sub_parsers):
    """
    """
    help = "Convert CSV to SQL."
    description = help + """
"""
    example = """
Examples:

    datapkg csv2sql example.cvs

"""
    parser = sub_parsers.add_parser(
        'file2db',
        help=help,
        description=description,
        epilog=example,
    )
    #
    parser.add_argument('-f', '--file', type=str, required=True)
    parser.add_argument('-d', '--db', dest='connection_string', required=True, help="""\
If present, an sqlalchemy connection string to use to directly execute generated SQL \
on a database.""")
    parser.add_argument('-s', '--storage_host', type=str, default=None)
    parser.add_argument('--debug', action='store_true', default=False)
    #
    parser.add_argument('--sep', type=str, default='\t')
    parser.add_argument('--nrows', type=int,
                        help='Number of rows to read when figuring out dtypes.')
    parser.add_argument('--skiprows', type=int, default=0,
                        help='Number of rows other than the header row.')
    parser.add_argument('--na_values', type=str, default=['', '.'])
    parser.set_defaults(func=_file2db)


def main():
    parser = argparse.ArgumentParser(
        prog='datapkg',
    )
    sub_parsers = parser.add_subparsers(
        title='command',
        help=''
    )
    configure_file2db_parser(sub_parsers)
    # configure_xxx_parser(sub_parsers)
    args = parser.parse_args()
    if 'func' not in args.__dict__:
        args = parser.parse_args(['--help'])
    args.func(args)


if __name__ == '__main__':
    import sys
    sys.exit(main())
