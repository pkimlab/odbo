import argparse
import logging
import os.path as op
from textwrap import dedent

from .connection import MySQLConnection

logger = logging.getLogger(__name__)


def _file2db(args):
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger(op.dirname(__file__)).setLevel(logging.DEBUG)
    db = MySQLConnection(
        connection_string=args.connection_string,
        # NOTEBOOK_NAME
        shared_folder=op.dirname(args.file),
        # os.environ['STG_SERVER_IP']
        storage_host=args.storage_host,
        echo=args.debug,
    )
    db.import_file(
        file=op.abspath(args.file),
        # **vargs
        sep=args.sep,
        skiprows=args.skiprows,
        na_values=args.na_values,
    )


def configure_file2db_parser(sub_parsers):
    """
    """
    help = "Convert CSV to SQL."
    description = help + "\n"

    example = dedent("""
        Examples:

            odbo csv2sql example.cvs

        """)
    parser = sub_parsers.add_parser(
        'file2db',
        help=help,
        description=description,
        epilog=example,
    )
    #
    parser.add_argument('-f', '--file', type=str, required=True)
    parser.add_argument(
        '-d',
        '--db',
        dest='connection_string',
        required=True,
        help=dedent("""\
            If present, an sqlalchemy connection string to use to directly execute generated SQL
            on a database.""").replace('\n', ' '),
    )
    parser.add_argument('-s', '--storage_host', type=str, default=None)
    parser.add_argument('--debug', action='store_true', default=False)
    #
    parser.add_argument('--sep', type=str, default='\t')
    parser.add_argument(
        '--skiprows', type=int, default=0, help='Number of rows other than the header row.')
    parser.add_argument('--na_values', type=str, default=['', '.'])
    parser.set_defaults(func=_file2db)


def main():
    parser = argparse.ArgumentParser(prog='odbo',)
    sub_parsers = parser.add_subparsers(title='command', help='')
    configure_file2db_parser(sub_parsers)
    # configure_xxx_parser(sub_parsers)
    args = parser.parse_args()
    if 'func' not in args.__dict__:
        args = parser.parse_args(['--help'])
    args.func(args)


if __name__ == '__main__':
    import sys
    sys.exit(main())
