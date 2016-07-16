import os.path as op
import pep8
import glob

PKG_ROOT_DIR = op.dirname(op.dirname(op.abspath(__file__)))


def test_pep8_compliance():
    """Test that we conform to the PEP8 standard."""
    pep8style = pep8.StyleGuide(
        config_file=op.join(PKG_ROOT_DIR, 'setup.cfg'),
        path=PKG_ROOT_DIR)
    list_of_files = glob.glob(op.join(PKG_ROOT_DIR, '**', '*.py'), recursive=True)
    print(list_of_files)
    result = pep8style.check_files(list_of_files)
    assert result.total_errors == 0