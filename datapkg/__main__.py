import argparse
from . import csv2sql


def configure_csv2sql_parser(sub_parsers):
    help = "Convert CSV to SQL."
    description = help + """
"""
    example = """
Examples:

    datapkg csv2sql example.cvs

"""
    parser = sub_parsers.add_parser(
        'csv2sql',
        help=help,
        description=description,
        epilog=example,
    )
    parser = csv2sql.configure_csv2sql_parser(parser)
    return parser


def main():
    parser = argparse.ArgumentParser(
        prog='datapkg',
    )
    sub_parsers = parser.add_subparsers(
        title='command',
        help=''
    )
    configure_csv2sql_parser(sub_parsers)
    args = parser.parse_args()
    if 'func' not in args.__dict__:
        args = parser.parse_args(['--help'])
    args.func(args)


if __name__ == '__main__':
    main()
