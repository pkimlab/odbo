"""Format (compressed) CSV file for import into an SQL database using Python.

- This script seems to run ~1.6 times slower than the `bash` version.
- PyPy runs ~0.97 times faster, so not worth it.
"""
from __future__ import print_function
import os
import os.path as op
import gzip
import bz2
import re
import logging
from datapkg import format_unprintable

logger = logging.getLogger(__name__)


def decompress(
        infile, sep='\t', na_values=None, extra_substitutions=None, use_tmp=False, outfile=None):
    """Decompress `infile` to produce a file with name `${infile}.tmp`.

    Parameters
    ----------
    outfile : str | None
        The name of the (decompressed) output file. If None, use `${infile}.tmp`.
    """
    ext = op.splitext(infile)[-1]
    SUPPORTED_EXTENSIONS = ['.gz', '.bz2']

    # File is not compressed, do nothing
    if ((ext not in SUPPORTED_EXTENSIONS) and
            (not na_values or na_values == ['\\N']) and
            (not extra_substitutions)):
        logger.debug("No need to process input file '{}'".format(infile))
        return infile

    if outfile is None:
        outfile = infile + '.tmp'

    if op.isfile(outfile):
        logger.debug("Decompressed file '{}' already exists!")
        if use_tmp:
            logger.debug("Using existing file...")
            return outfile
        else:
            logger.debug("Removing existing file...")
            os.remove(outfile)

    # Uncompress file, applying function `fn`
    logger.debug("Uncompressing file '{}' into '{}'...".format(infile, outfile))
    fn = get_csv_line_formatter(sep, na_values, extra_substitutions)
    try:
        ifh = _open_compressed_file(infile, 'rb')
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
    assert op.isfile(outfile)
    return outfile


def _open_compressed_file(filename, mode='rb'):
    """Return file handle for the file `filename`.

    Inferr compression from the extension.
    """
    if filename.endswith('.gz'):
        return gzip.open(filename, mode)
    elif filename.endswith('.bz2'):
        return bz2.open(filename, mode)
    else:
        return open(filename, mode)


def get_csv_line_formatter(sep, na_values=None, extra_substitutions=None):
    r""".

    Examples
    --------
    >>> formatter = get_csv_line_formatter(',', ['X', '', 'NA', '\\N'])
    >>> print(formatter(b"X,,X,\\N,N,\n,N,NA,NA,,").decode())
    \N,\N,\N,\N,N,\N
    \N,N,\N,\N,\N,\N
    """
    na_values = list(na_values) if na_values is not None else []
    extra_substitutions = list(extra_substitutions) if extra_substitutions is not None else []

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

    if not na_values or na_values == ['\\N']:
        def rep_null(line):
            return line
    else:
        rep_null = _get_rep_null(sep, na_values, extra_substitutions)

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


def _get_rep_null(sep, na_values, extra_substitutions):
    """Returns a function which replaces `na_values` with '\\N'.
    """
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

    return rep_null


def main(infile, outfile, sep='\t', na_values=[], extra_substitutions=[]):
    if not na_values:
        na_values = ['', '\\N', '.', 'na']

    outfile = decompress(
        infile=infile,
        sep=sep,
        na_values=na_values,
        extra_substitutions=extra_substitutions,
        outfile=outfile)
    assert op.isfile(outfile)
    return 0


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
