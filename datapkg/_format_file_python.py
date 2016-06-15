"""Format (compressed) CSV file for import into an SQL database using Python.

- This script seems to run ~1.6 times slower than the `bash` version.
- PyPy runs ~0.97 times faster, so not worth it.
"""
from __future__ import print_function
import os.path as op
import gzip
import bz2
import re
import logging
from ascommon.py_tools import format_unprintable

logger = logging.getLogger(__name__)


def decompress(infile, outfile, sep='\t', na_values=None, extra_substitutions=None):
    """."""
    ext = op.splitext(infile)[-1]
    SUPPORTED_EXTENSIONS = ['.gz', '.bz2']

    # File is not compressed, do nothing
    if ((ext not in SUPPORTED_EXTENSIONS) and
            (not na_values or na_values == ['\\N']) and
            (not extra_substitutions)):
        logger.debug("File '{}' is not compressed...".format(infile))
        return

    # Uncompress file, applying function `fn`
    fn = get_csv_line_formatter(sep, na_values, extra_substitutions)
    logger.debug("Uncompressing file...".format(infile))
    try:
        if ext == '.gz':
            ifh = gzip.open(infile, 'rb')
        elif ext == 'bz2':
            ifh = bz2.open(infile, 'rb')
        else:
            ifh = open(infile, 'rb')
    except:
        raise
    else:
        with open(outfile, 'wb') as ofh:
            while True:
                data = ifh.read(64 * 1024 * 1024)
                if not data:
                    break
                ofh.write(fn(data))
    finally:
        ifh.close()


def get_csv_line_formatter(sep, na_values=None, extra_substitutions=None):
    r""".

    Examples
    --------
    >>> formatter = get_csv_line_formatter(',', ['X', '', 'NA', '\\N'])
    >>> print(formatter(b"X,,X,\\N,N,\n,N,NA,NA,,").decode())
    \N,\N,\N,\N,N,\N
    \N,N,\N,\N,\N,\N
    """
    na_values = na_values[:] if na_values is not None else []
    extra_substitutions = extra_substitutions[:] if extra_substitutions is not None else []

    if '\\N' in na_values:
        na_values.remove('\\N')

    if not na_values and not extra_substitutions:
        # Nothing to sed
        return lambda x: x

    # # Header
    # def rep_header(line):
    #     if line[:2] == '##':
    #         return ''
    #     else:
    #         return line

    # def format_unprintable(string):
    #     return string
    #
    if not na_values or na_values == ['\\N']:
        def rep_null(line):
            return line
    else:
        RE1 = re.compile(
            format_unprintable(
                '|'.join('{0}{1}{0}'.format(sep, na_value) for na_value in na_values))
            .encode('utf-8'))
        RE1_OUT = format_unprintable('{0}{1}{0}'.format(sep, '\\N')).encode('utf-8')

        RE2 = re.compile(
            format_unprintable(
                '|'.join('^{1}{0}'.format(sep, na_value) for na_value in na_values))
            .encode('utf-8'))
        RE2_OUT = format_unprintable('{1}{0}'.format(sep, '\\N')).encode('utf-8')

        RE3 = re.compile(
            format_unprintable(
                '|'.join('{0}{1}$'.format(sep, na_value) for na_value in na_values))
            .encode('utf-8'))
        RE3_OUT = format_unprintable('{0}{1}'.format(sep, '\\N')).encode('utf-8')

        RE4 = re.compile(
            format_unprintable(
                '|'.join('\r?\n{1}{0}'.format(sep, na_value) for na_value in na_values))
            .encode('utf-8'))
        RE4_OUT = format_unprintable('\n{1}{0}'.format(sep, '\\N')).encode('utf-8')

        RE5 = re.compile(
            format_unprintable(
                '|'.join('{0}{1}\r?\n'.format(sep, na_value) for na_value in na_values))
            .encode('utf-8'))
        RE5_OUT = format_unprintable('{0}{1}\n'.format(sep, '\\N')).encode('utf-8')

        def rep_null(line):
            line = RE1.sub(RE1_OUT, line)
            line = RE1.sub(RE1_OUT, line)
            line = RE2.sub(RE2_OUT, line)
            line = RE3.sub(RE3_OUT, line)
            line = RE4.sub(RE4_OUT, line)
            line = RE5.sub(RE5_OUT, line)
            for RE, RE_OUT in extra_substitutions:
                line = RE.sub(RE_OUT, line)
            return line

    # Separator
    if sep == '\t':
        def rep_sep(line):
            return line
    else:
        def rep_sep(line):
            return line.replace(sep.encode('utf-8'), b'\t')

    # Final function
    def csv_line_formatter(line):
        return rep_null(line)
        # return rep_sep(rep_null(rep_header(line)))

    return csv_line_formatter


def main(infile, outfile, sep='\t', na_values=[], extra_substitutions=[]):
    if not na_values:
        na_values = ['', '\\N', '.', 'na']

    decompress(
        infile=infile,
        outfile=outfile,
        sep=sep,
        na_values=na_values,
        extra_substitutions=extra_substitutions)


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--infile', type=str)
    parser.add_argument('-o', '--outfile', type=str)
    args = parser.parse_args()
    if args.outfile is None:
        args.outfile = op.splitext(args.infile)[0] + '.tmp'
    main(args.infile, args.outfile)
