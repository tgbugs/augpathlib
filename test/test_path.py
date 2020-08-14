import os
import sys
import unittest
from pathlib import PurePosixPath
import pytest
from augpathlib import swap
from augpathlib import exceptions as exc
from augpathlib import AugmentedPath, LocalPath
from augpathlib import SymlinkCache, PrimaryCache
from augpathlib import PathMeta
from augpathlib import PathMeta
from augpathlib.meta import _PathMetaAsSymlink, _PathMetaAsXattrs
from .common import (log,
                     onerror,
                     project_path,
                     temp_path,
                     test_base,
                     test_path,
                     TestPathHelper,
                     LocalPathTest,
                     CachePathTest,)

SymlinkCache._local_class = AugmentedPath  # have to set a default

class Helper:
    @classmethod
    def setUpClass(cls):
        if not test_base.exists():
            test_base.mkdir()

    @classmethod
    def tearDownClass(cls):
        if test_base.exists():
            test_base.rmtree(onerror=onerror)

    def setUp(self):
        self.test_link = AugmentedPath(test_base, 'evil-symlink')  # FIXME random needed ...
        if self.test_link.is_symlink():
            self.test_link.unlink()

        self.test_link.symlink_to('hello/there')

        self.test_path = AugmentedPath(test_base, 'aug-testpath')  # FIXME random needed ...
        if self.test_path.exists():
            self.test_path.rmtree(onerror=onerror)

    def tearDown(self):
        if self.test_link.is_symlink():
            self.test_link.unlink()

        if self.test_path.exists():
            self.test_path.rmtree(onerror=onerror)


class TestAugPath(Helper, unittest.TestCase):

    def test_is_dir_symlink(self):
        assert not self.test_link.is_dir()

    def test_rmtree(self):
        self.test_path.mkdir()
        d = (self.test_path / 'heh')
        d.mkdir()
        f = (self.test_path / 'other')
        f.touch()
        f.chmod(0o0000)
        self.test_path.rmtree(onerror=onerror)

    def test_rmtree_ignore(self):
        try:
            self.test_path.rmtree(onerror=onerror)
            raise AssertionError('should fail')
        except FileNotFoundError as e:
            pass

        # this doesn't test passing deeper ...
        self.test_path.rmtree(ignore_errors=True, onerror=onerror)

    def test_rmtree_symlinks(self):
        """ make sure we don't rmtree through symlinks """
        tp = self.test_path
        tp.mkdir()
        d = tp / 'dir'
        t = tp / 'dir/target'
        f = tp / 'dir/target/file'
        so = tp / 'dir/source'
        s = tp / 'dir/source/symlink'
        d.mkdir()
        t.mkdir()
        f.touch()
        so.mkdir()
        s.symlink_to(t)
        try:
            s.rmtree()
            assert False, 'should have failed!'
        except OSError:
            pass

        so.rmtree()
        assert not so.exists()
        assert f.exists() and t.exists()
        self.test_path.rmtree(onerror=onerror)

    def test_relative_path_from(self):
        p1 = self.test_path / 'a' / 'b' / 'c' / 'd'
        p2 = self.test_path / 'e' / 'f' / 'g' / 'h'
        e1 = AugmentedPath('..', '..', '..', 'a', 'b', 'c', 'd')
        e2 = AugmentedPath('..', '..', '..', 'e', 'f', 'g', 'h')
        p1rfp2 = p1.relative_path_from(p2)
        p2rfp1 = p2.relative_path_from(p1)
        assert e1 == p1rfp2, p1rfp2
        assert e2 == p2rfp1, p2rfp1


@pytest.mark.skipif(os.name == 'nt', reason='no easy way to get libmagic on windows')
class TestMimetype(Helper, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.tf = self.test_path / 'some-text.txt'
        self.test_path.mkdir()
        with open(self.tf, 'wt') as f:
            f.write('hello')

    def test_mimetype(self):
        mt = self.tf.mimetype
        assert mt == 'text/plain', mt

    def test_magic_mimetype(self):
        mmt = self.tf._magic_mimetype
        assert mmt == 'text/plain', mmt


class TestAugPathCopy(Helper, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.test_path.mkdir()
        self.source_d = self.test_path / 'source-dir'
        self.target_d = self.test_path / 'target-dir'
        self.source_f = self.test_path / 'source-file'
        self.target_f = self.test_path / 'target-file'
        self.source_d.mkdir()
        self.target_d.mkdir()
        self.source_f.touch()
        self.target_f.touch()

    def test_copy_outto(self):
        #self.source_d.copy_outto(self.target_d)  # copytree not currently supported
        self.source_f.copy_outto(self.target_d)

    def test_copy_outto_fail_d(self):
        try:
            self.source_d.copy_outto(self.target_f)
            raise AssertionError('should have failed with NotADirectoryError')
        except NotADirectoryError:
            pass

    def test_copy_outto_fail_f(self):
        try:
            self.source_f.copy_outto(self.target_f)
            raise AssertionError('should have failed with NotADirectoryError')
        except NotADirectoryError:
            pass

    @pytest.mark.skip('copytree not implemented')
    def test_copy_outto_fail_existing_d(self):
        self.source_d.copy_outto(self.target_d)
        try:
            self.source_d.copy_outto(self.target_d)
            raise AssertionError('should have failed with FileExistsError')
        except FileExistsError:
            pass

    def test_copy_outto_fail_existing_f(self):
        self.source_f.copy_outto(self.target_d)
        try:
            self.source_f.copy_outto(self.target_d)
            raise AssertionError('should have failed with FileExistsError')
        except exc.PathExistsError:
            pass

    def test_copy_infrom(self):
        #self.target_d.copy_infrom(self.source_d)  # copytree not implemented
        self.target_d.copy_infrom(self.source_f)
    
    @pytest.mark.skip('copytree not implemented')
    def test_copy_infrom_fail_d(self):
        try:
            self.target_f.copy_infrom(self.source_d)
            raise AssertionError('should have failed with NotADirectoryError')
        except NotADirectoryError:
            pass

    def test_copy_infrom_fail_f(self):
        try:
            self.target_f.copy_infrom(self.source_f)
            raise AssertionError('should have failed with NotADirectoryError')
        except NotADirectoryError:
            pass


class TestAugPathSwap(Helper, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.test_path.mkdir()
        self.source_d = self.test_path / 'source-dir'
        self.target_d = self.test_path / 'target-dir'
        self.source_d.mkdir()
        self.target_d.mkdir()

        self.f1 = self.source_d / 'f1'
        self.f2 = self.target_d / 'f2'
        self.f1.touch()
        self.f2.touch()

    def _doit(self, thunk):
        assert (self.source_d / self.f1.name).exists()
        assert (self.target_d / self.f2.name).exists()
        assert self.f1.exists()
        assert self.f2.exists()
        thunk()
        assert (self.source_d / self.f2.name).exists()
        assert (self.target_d / self.f1.name).exists()
        assert not self.f1.exists()
        assert not self.f2.exists()

    @pytest.mark.skipif(sys.platform != 'linux', reason='not implemented')
    def test_swap_linux(self):
        self._doit(lambda :swap.swap_linux(self.source_d, self.target_d))

    @pytest.mark.skipif(sys.platform != 'linux', reason='not implemented')
    def test_swap(self):
        self._doit(lambda :self.source_d.swap(self.target_d))

    def test_swap_carefree(self):
        self._doit(lambda :self.source_d.swap_carefree(self.target_d))


class TestACachePath(unittest.TestCase):
    def setUp(self):
        if test_path.is_symlink():
            test_path.unlink()

        if test_path.exists() and test_path.is_dir():
            test_path.rmdir()

    def test_0_exists(self):
        log.debug(CachePathTest.anchor)
        assert not CachePathTest.anchor.is_symlink()
        assert CachePathTest.anchor.meta is None

    def test_1_create(self):
        wat = CachePathTest(test_path, meta=PathMeta(id='0'))
        log.debug(wat)
        assert wat.meta

    def test_2_create_dir(self):
        test_path.mkdir()
        wat = CachePathTest(test_path, meta=PathMeta(id='0'))
        log.debug(wat)
        assert wat.meta

    def test_cache_init_dir(self):
        if test_path.exists():  # in case something went wrong with a previous test
            shutil.rmtree(test_path, onerror=onerror)

        test_path.mkdir()
        assert test_path.exists()
        assert test_path.is_dir()
        test_path.cache_init('0')
        assert test_path.cache
        assert test_path.cache.meta


class TestCacheSparse(unittest.TestCase):
    _test_class = CachePathTest
    sandbox = test_base / 'sparse-sandbox'
    @classmethod
    def setUpClass(cls):
        cls.sandbox.mkdir(parents=True)

    @classmethod
    def tearDownClass(cls):
        cls.sandbox.rmtree(onerror=onerror)

    def setUp(self):
        self.dir = self._test_class(self.sandbox, 'some-dir', meta=PathMeta(id='0'))
        if self.dir.exists():
            self.dir.rmtree(onerror=onerror)
        elif self.dir.is_symlink():
            self.dir.unlink()
        self.dir.mkdir()

        self.file = self._test_class(self.sandbox, 'some-file', meta=PathMeta(id='1'))
        if self.file.exists() or self.file.is_symlink():
            self.file.unlink()
        self.file.touch()

    def tearDown(self):
        self.dir.rmtree(onerror=onerror)
        self.file.unlink()

    def test_sparse(self):
        test_file = self._test_class(self.dir, 'more-test', meta=PathMeta(id='3'))
        test_file.unlink()  # FIXME fix bad Cache constructor behavior already
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


class TestPathMeta(unittest.TestCase):
    prefix = None

    def setUp(self):
        self.path = LocalPathTest(project_path)

        self.test_path = LocalPathTest(test_base, 'testpath')  # FIXME random needed ...
        if self.test_path.is_symlink():
            self.test_path.unlink()

    def _test_getattr_size_hr(self):
        pm = PathMeta(size=1000000000000000)
        woo = getattr(pm, 'size.hr')

    def test_neg__neg__(self):
        pm = PathMeta(id='lol')
        assert pm

    def test___neg__(self):
        pm = PathMeta()
        assert not pm, set(pm.__dict__.values())

    def test_xattrs_roundtrip(self):
        # TODO __kwargs ...
        pm = self.path.meta
        xattrs = pm.as_xattrs(self.prefix)
        log.debug(xattrs)
        # FIXME actually write these to disk as well?
        new_pm = PathMeta.from_xattrs(xattrs, self.prefix)
        msg = '\n'.join([f'{k!r} {v!r} {getattr(new_pm, k)!r}' for k, v in pm.items()])
        assert new_pm == pm, msg
        #'\n'.join([str((getattr(pm, field), getattr(new_pm, field)))
        #for field in _PathMetaAsXattrs.fields])

    def test_metastore_roundtrip(self):
        pm = self.path.meta
        ms = pm.as_metastore(self.prefix)
        # FIXME actually write these to disk as well?
        new_pm = PathMeta.from_metastore(ms, self.prefix)
        assert new_pm == pm, '\n'.join([str((getattr(pm, field), getattr(new_pm, field)))
                                        for field in tuple()])  # TODO

    def test_symlink_roundtrip(self):
        meta = PathMeta(id='N:helloworld:123', size=10, checksum=b'1;o2j\x9912\xffo3ij\x01123,asdf.')
        path = self.test_path
        path._cache = SymlinkCache(path, meta=meta)
        path.cache.meta = meta
        new_meta = path.cache.meta
        path.unlink()
        msg = '\n'.join([f'{k!r} {v!r} {getattr(new_meta, k)!r}' for k, v in meta.items()])
        assert meta == new_meta, msg

    def _test_symlink_roundtrip_weird(self):
        path = LocalPathTest(test_base, 'testpath')  # FIXME random needed ...
        meta = PathMeta(id='N:helloworld:123', size=10, checksum=b'1;o2j\x9912\xffo3ij\x01123,asdf.')
        pure_symlink = PurePosixPath(path.name) / meta.as_symlink()
        path.symlink_to(pure_symlink)
        try:
            cache = SymlinkCache(path)
            new_meta = cache.meta
            msg = '\n'.join([f'{k!r} {v!r} {getattr(new_meta, k)!r}' for k, v in meta.items()])
            assert meta == new_meta, msg
        finally:
            path.unlink()

    def test_parts_roundtrip(self):
        pmas = _PathMetaAsSymlink()
        lpm = self.path.meta
        bpm = PathMeta(id='N:helloworld:123', size=10, checksum=b'1;o2j\x9912\xffo3ij\x01123,asdf.')
        bads = []
        for pm in (lpm, bpm):
            symlink = pm.as_symlink()
            log.debug(symlink)
            new_pm = pmas.from_parts(symlink.parts)
            #corrected_new_pm = PurePosixPath()
            if new_pm != pm:
                bads += ['\n'.join([str((getattr(pm, field), getattr(new_pm, field)))
                                    for field in ('id',) + _PathMetaAsSymlink.order
                                    if not (getattr(pm, field) is getattr(new_pm, field) is None)]),
                         f'{pm.__reduce__()}\n{new_pm.__reduce__()}']

        assert not bads, '\n===========\n'.join(bads)


class TestPrefix(TestPathMeta):
    prefix = 'prefix'


class TestPrefixEvil(TestPathMeta):
    prefix = 'prefix.'


class TestContext(unittest.TestCase):
    def setUp(self):
        if not temp_path.exists():
            temp_path.mkdir()

    def tearDown(self):
        temp_path.rmtree(onerror=onerror)

    def test_context(self):
        start = AugmentedPath.cwd()
        target = AugmentedPath(temp_path).resolve()  # resolve needed for osx
        distractor = AugmentedPath('~/').expanduser()
        assert temp_path.is_dir()
        with target:
            target_cwd = AugmentedPath.cwd()
            distractor.chdir()
            distractor_cwd = AugmentedPath.cwd()

        end = AugmentedPath.cwd()
        assert target == target_cwd, 'with target: failed'
        assert distractor == distractor_cwd, 'distractor cwd failed'
        assert start == end, 'it would seem that the distractor got us'
        assert start != target != distractor


class TestIdZero(TestPathHelper, unittest.TestCase):
    def test(self):
        zt = LocalPathTest(test_path) / 'zero-test'
        cache = CachePathTest(zt, meta=PathMeta(id='0'))
        assert cache.meta


class TestUpdateMeta(unittest.TestCase):
    def test_update(self):
        old = PathMeta(id='0', size=10, file_id=1)
        new = PathMeta(id='0', size=10, checksum='asdf')
        changed, merged = PrimaryCache._update_meta(old, new)
        test_value = PathMeta(id='0', size=10, file_id=1, checksum='asdf')
        assert merged == test_value, test_value.as_pretty_diff(merged)


class TestActuallyLocalPath(unittest.TestCase):
    def setUp(self):
        LocalPath

    def tearDown(self):
        LocalPath

    def test_no_cache_class(self):
        lp = LocalPath(__file__)
        p = lp.parent
        rc = list(p.rchildren)
        assert rc, 'hrm'
