import unittest
from augpathlib import exceptions as exc
from .common import TestPathHelper, LocalPathTest, CachePathTest, RemotePathTest
from .common import log


class TestMove(TestPathHelper, unittest.TestCase):
    def _mkpath(self, path, time, is_dir):
        if path.exists():
            return

        if not path.parent.exists():
            yield from self._mkpath(path.parent, time, True)

        if is_dir:
            path.mkdir()
        else:
            path.touch()

        yield path.cache_init(path.metaAtTime(time))

    def _test_move(self, source, target, target_exists=False):
        s = self.test_path / source
        t = self.test_path / target
        #remote = RemotePathTest.invAtTime(1)
        caches = list(self._mkpath(s, 1, int(s.metaAtTime(1).id) in RemotePathTest.dirs))
        if target_exists:  # FIXME and same id vs and different id
            target_caches = list(self._mkpath(t, 2, int(t.metaAtTime(2).id) in RemotePathTest.dirs))

        cache = caches[-1]
        meta = t.metaAtTime(2)
        log.debug(f'{source} -> {target} {cache.meta} {meta}')
        cache.move(target=t, meta=meta)
        assert t.cache.id == RemotePathTest.invAtTime(t, 2)

    def test_0_0_test_cache_local(self):
        c = self.test_path.cache
        assert hasattr(c, '_local_class')
        assert c.local

    def test_0_dir_moved(self):
        source = 'a.e'
        target = 'b.e'
        self._test_move(source, target)

    def test_1_file_moved(self):
        source = 'c.e'
        target = 'd.e'
        self._test_move(source, target)

    def test_2_parent_moved(self):
        source = 'ee/ff/gg'
        target = 'hh/ff/gg'
        self._test_move(source, target)

    def test_3_parents_moved(self):
        source = 'ii/jj/kk'
        target = 'll/mm/kk'
        self._test_move(source, target)

    def test_4_all_moved(self):
        source = 'nn/oo/pp'
        target = 'qq/rr/ss'
        self._test_move(source, target)

    def test_5_onto_self(self):
        source = 't.e'
        target = 't.e'
        self._test_move(source, target)

    def test_6_onto_different(self):
        source = 'a.e'
        target = 't.e'
        try:
            self._test_move(source, target)
            raise AssertionError('should have failed')
        except exc.PathExistsError:
            pass


class TestMoveTargetExists(TestMove):
    def _test_move(self, source, target):
        super()._test_move(source, target, target_exists=True)
        # since all the ids match this should work ...
        #try:
            #raise AssertionError('should have failed')
        #except exc.PathExistsError:
            #pass
