import unittest
import augpathlib as aug
from .common import test_base, onerror

sandbox = test_base / 'eat-sandbox'


class EatPath(aug.EatHelper, aug.AugmentedPath): pass
EatPath._bind_flavours()


class TestEat(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sandbox.mkdir(parents=True)

    @classmethod
    def tearDownClass(cls):
        sandbox.rmtree(onerror=onerror)
    
    def setUp(self):
        self.dir = EatPath(sandbox, 'some-dir')
        if self.dir.exists():
            self.dir.rmtree(onerror=onerror)
        self.dir.mkdir()

        self.file = EatPath(sandbox, 'some-file')
        if self.file.exists() :
            self.file.unlink()
        self.file.touch()

    def tearDown(self):
        self.dir.rmtree(onerror=onerror)
        self.file.unlink()

    def test_dir(self):
        self.dir.setxattr('key', b'value')
        test = self.dir.xattrs()
        assert test

    def test_file(self):
        self.file.setxattr('key', b'value')
        test = self.file.xattrs()
        assert test
