import unittest
from augpathlib.utils import FileSize

class TestFileSize(unittest.TestCase):
    def test_0_getattr(self):
        s = FileSize(1000000000000000)
        getattr(s, 'hr')

    def test_1_str(self):
        s = FileSize(1)
        ss = str(s)

    def test_2_repr(self):
        s = FileSize(1)
        sr = str(s)
