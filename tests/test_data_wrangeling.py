from data_wrangling import __version__
from data_wrangling import parse_name_with_id


def test_version():
    assert __version__ == "0.1.0"


def test_parse_name_with_id():
    assert ("", 43035, "_r") == parse_name_with_id("043035_r")