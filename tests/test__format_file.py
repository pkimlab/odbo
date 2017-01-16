import os
import os.path as op
import tempfile
import time
import pandas as pd
import pytest
import logging
import odbo

logger = logging.getLogger(__name__)


def test_csv_line_formatter():
    csv_line_formatter = (
        odbo._format_file_python.get_csv_line_formatter(sep=',', na_values=['', 'NS', '\\N'])
    )
    expected = b"\\N,\\N,\\N,\\N,\\N,\\N\n"
    actual = csv_line_formatter(b",,NS,\\N,,\n")
    assert expected == actual, (expected, actual)


@pytest.mark.parametrize("_format_file", [odbo._format_file_bash, odbo._format_file_python])
def test__format_file(_format_file):
    """Make sure that `_format_file` correctly converts null values to '\\N'."""
    infile = op.join(op.splitext(__file__)[0], 'CosmicSample.tsv.gz')

    tf, outfile = tempfile.mkstemp()
    logger.info(outfile)

    df1 = pd.read_csv(infile, sep='\t', na_values=['', 'NS', '\\N'], low_memory=False)
    _start_time = time.time()
    _format_file.main(infile, outfile, na_values=['', 'NS', '\\N'])
    _end_time = time.time()
    df2 = pd.read_csv(outfile, sep='\t', na_values=['\\N'], low_memory=False)

    logger.info("Conversion took {} seconds...".format(_end_time - _start_time))
    assert (df1.fillna(0) == df2.fillna(0)).all().all()

    os.remove(outfile)
