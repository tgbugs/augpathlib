import unittest
from augpathlib.utils import FileSize

class TestFileSize(unittest.TestCase):
    def test_getattr(self):
        s = FileSize(1000000000000000)
        getattr(s, 'hr')
