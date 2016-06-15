"""Format (compressed) CSV file for import into an SQL database using Linux system commands."""
import os.path as op
import logging
from datapkg import run_command, format_unprintable

logger = logging.getLogger(__name__)


def decompress(infile, outfile, sep='\t', na_values=None, extra_substitutions=None):
    """."""
    ext = op.splitext(infile)[-1]
    if ext == '.gz':
        executable = 'gzip -dc'
    elif ext == '.bz2':
        executable = 'bz2 -dc'
    else:
        executable = 'cat'

    sed_command = get_sed_command(sep, na_values, extra_substitutions)

    system_command = (
        "{executable} '{infile}' {sed_commad} > '{outfile}'".format(
            executable=executable,
            infile=infile,
            sed_commad=('| ' + sed_command) if sed_command else '',
            outfile=outfile,
        )
    )
    logger.debug(system_command)
    run_command(system_command, shell=True)  # NB: sed is CPU-bound, no need to do remotely
    assert op.isfile(outfile)


def get_sed_command(sep='\t', na_values=None, extra_substitutions=None):
    """."""
    na_values = na_values[:] if na_values is not None else []
    extra_substitutions = extra_substitutions[:] if extra_substitutions is not None else []

    if '\\N' in na_values:
        na_values.remove('\\N')

    if not na_values and not extra_substitutions:
        return ''

    system_command_head = "sed "
    system_command_body = ""
    system_command_body += ''.join([r"-e '{}' ".format(x) for x in extra_substitutions])
    for na_value in na_values:
        if na_value and na_value in "$.*[\\]^'\"":
            na_value = '\{}'.format(na_value)
        else:
            na_value = format_unprintable(na_value)
        system_command_body += (
            r"-e 's/{0}{1}{0}/{0}\\N{0}/g' "
            r"-e 's/{0}{1}{0}/{0}\\N{0}/g' "
            r"-e 's/^{1}{0}/\\N{0}/g' "
            r"-e 's/{0}{1}$/{0}\\N/g' "
            r"-e 's/{0}{1}\r$/{0}\\N/g' "
            .format(format_unprintable(sep), na_value)
        )
    return system_command_head + system_command_body


def main(infile, outfile, sep='\t', na_values=(), extra_substitutions=()):
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
