import os
import unittest
import pytest
import augpathlib as aug
from .common import test_base, onerror

sandbox = test_base / 'eat-sandbox'


class EatPath(aug.EatPath, aug.AugmentedPath): pass
EatPath._bind_flavours()


class EatXopenPath(aug.EatPath, aug.XopenPath, aug.AugmentedPath): pass
EatXopenPath._bind_flavours()


class TestEat(unittest.TestCase):
    _test_class = EatPath
    @classmethod
    def setUpClass(cls):
        sandbox.mkdir(parents=True)

    @classmethod
    def tearDownClass(cls):
        sandbox.rmtree(onerror=onerror)
    
    def setUp(self):
        self.dir = self._test_class(sandbox, 'some-dir')
        if self.dir.exists():
            self.dir.rmtree(onerror=onerror)
        self.dir.mkdir()

        self.file = self._test_class(sandbox, 'some-file')
        if self.file.exists():
            self.file.unlink()
        self.file.touch()

    def tearDown(self):
        self.dir.rmtree(onerror=onerror)
        self.file.unlink()

    @pytest.mark.skipif(os.name != 'nt', reason='This ADS behavior is windows only')
    def test_dir_simple(self):
        stream = self.dir._stream('wat')
        tv = b'wat-value'
        with open(stream, 'wb') as f:
            f.write(tv)

        with open(stream, 'rb') as f:
            test = f.read()

        assert test == tv
        streams = list(self.dir._streams)
        assert streams

    def test_pathological(self):
        hrm = [
            'a',
            #'/b',
            'c/d',
            'e/f/',
            #'/g/',
            #'/h/i',
            #'/j/k/',
        ]
        asdf = [self.dir / p for p in hrm]
        bads = []
        for p in asdf:
            p.mkdir(parents=True)
            p.setxattr('test.hello', b'test')
            if p.getxattr('test.hello') != b'test':
                bads.append(p)

        assert not bads, bads

    def test_dir(self):
        self.dir.setxattr('key', b'value')
        test = self.dir.xattrs()
        assert test

    def test_dir_relative(self):
        self.dir.setxattr('key', b'value')
        with self.dir:
            test = self.dir.xattrs()

        assert test

    def test_dir_relative_dot(self):
        self.dir.setxattr('key', b'value')
        with self.dir:
            test = self._test_class('.').xattrs()

        assert test

    def test_file(self):
        self.file.setxattr('key', b'value')
        test = self.file.xattrs()
        assert test

    def test_sparse(self):
        test_file = self.dir / 'more-test'
        test_file.touch()

        assert not self.file.is_sparse()
        assert not self.dir.is_sparse()
        assert not test_file.is_sparse()

        self.dir._mark_sparse()
        assert not self.file.is_sparse()
        assert self.dir.is_sparse()
        assert test_file.is_sparse()

        self.dir._clear_sparse()
        assert not self.file.is_sparse()
        assert not self.dir.is_sparse()
        assert not test_file.is_sparse()


class TestEatXopen(TestEat):
    _test_class = EatXopenPath
