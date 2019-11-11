import unittest
from pathlib import PurePosixPath
from augpathlib import AugmentedPath, LocalPath
from augpathlib import SymlinkCache, PrimaryCache
from augpathlib import PathMeta
from augpathlib.meta import _PathMetaAsSymlink, _PathMetaAsXattrs
from .common import (log,
                     onerror,
                     project_path,
                     temp_path,
                     test_base,
                     test_path,
                     TestPathHelper,
                     TestLocalPath,
                     TestCachePath,
                     TestRemotePath)

SymlinkCache._local_class = AugmentedPath  # have to set a default


class TestAugPath(unittest.TestCase):

    def setUp(self):
        if not test_base.exists():
            test_base.mkdir()

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


class TestACachePath(unittest.TestCase):
    def setUp(self):
        if test_path.is_symlink():
            test_path.unlink()

        if test_path.exists() and test_path.is_dir():
            test_path.rmdir()

    def test_0_exists(self):
        log.debug(TestCachePath.anchor)
        assert not TestCachePath.anchor.is_symlink()
        assert TestCachePath.anchor.meta is None

    def test_1_create(self):
        wat = TestCachePath(test_path, meta=PathMeta(id='0'))
        log.debug(wat)
        assert wat.meta

    def test_2_create_dir(self):
        test_path.mkdir()
        wat = TestCachePath(test_path, meta=PathMeta(id='0'))
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


class TestPathMeta(unittest.TestCase):
    prefix = None

    def setUp(self):
        self.path = TestLocalPath(project_path)

        self.test_path = TestLocalPath(test_base, 'testpath')  # FIXME random needed ...
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
        path = TestLocalPath(test_base, 'testpath')  # FIXME random needed ...
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
        target = AugmentedPath(temp_path)
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
        zt = TestLocalPath(test_path) / 'zero-test'
        cache = TestCachePath(zt, meta=PathMeta(id='0'))
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
