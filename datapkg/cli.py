import os.path as op
import argparse
import logging
import logging.config

import common

from .core import main


def configure_logger(debug=False):
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


if __name__ == '__main__':
    args = parse_args()
    common.configure_logging()
    #
    print(args)
    path = op.abspath(args.file)
    main(path, args.connection_string,
         sep=args.sep, nrows=args.nrows, skiprows=args.skiprows, na_values=args.na_values)
