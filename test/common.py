import os
import atexit
import shutil
import pathlib
from tempfile import gettempdir
import pytest
import augpathlib as aug
from augpathlib import exceptions as exc
from augpathlib import LocalPath
from augpathlib import PrimaryCache, RemotePath
from augpathlib import EatCache, SymlinkCache
from augpathlib import PathMeta
from augpathlib.utils import onerror_windows_readwrite_remove

aug.utils.log.setLevel('DEBUG')
log = aug.utils.log.getChild('test')

onerror = onerror_windows_readwrite_remove if os.name == 'nt' else None

_pid = os.getpid()
this_file = LocalPath(__file__)
temp_path = aug.AugmentedPath(gettempdir(), f'.augpathlib-testing-base-{_pid}')
project_path = this_file.parent / 'test_local/test_project'

# insurance in case some test forgets to clean up after itself
atexit.register(lambda : (shutil.rmtree(temp_path, onerror=onerror)
                          if temp_path.exists() else None))

SKIP_NETWORK = ('SKIP_NETWORK' in os.environ or
                'FEATURES' in os.environ and 'network-sandbox' in os.environ['FEATURES'])
skipif_no_net = pytest.mark.skipif(SKIP_NETWORK, reason='Skipping due to network requirement')


class LocalPathTest(LocalPath):
    def metaAtTime(self, time):
        # we are cheating in order to do this
        return PathMeta(id=self._cache_class._remote_class.invAtTime(self, time))


LocalPathTest._bind_flavours()


test_base = LocalPathTest(__file__).parent / f'test-base-{_pid}'
test_path = test_base / 'test-container'


class CachePathTest(PrimaryCache, EatCache):
    xattr_prefix = 'test'
    #_backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache


CachePathTest._bind_flavours()


class RemotePathTest(RemotePath):
    anchor = test_path
    ids = {0: anchor}  # time invariant
    dirs = {2, 3, 4, 8, 9, 11, 12, 13, 14, 16, 17, 18}
    index_at_time = {1: {1: anchor / 'a.e',

                         2: anchor / 'c.e',

                         3: anchor / 'ee',
                         4: anchor / 'ee/ff',
                         5: anchor / 'ee/ff/gg',

                         8: anchor / 'ii',
                         9: anchor / 'ii/jj',
                         10: anchor / 'ii/jj/kk',

                         13: anchor / 'nn',
                         14: anchor / 'nn/oo',
                         15: anchor / 'nn/oo/pp',

                         18: anchor / 't.e',},
                     2: {1: anchor / 'b.e',

                         2: anchor / 'd.e',

                         3: anchor / 'hh/',
                         4: anchor / 'hh/ff',
                         5: anchor / 'hh/ff/gg',

                         11: anchor / 'll',
                         12: anchor / 'll/mm',
                         10: anchor / 'll/mm/kk',

                         16: anchor / 'qq',
                         17: anchor / 'qq/rr',
                         15: anchor / 'qq/rr/ss',

                         18: anchor / 't.e',

                         19: anchor / 'u.e',}}

    for ind in index_at_time:
        index_at_time[ind].update(ids)

    test_time = 2

    def __init__(self, thing_with_id, cache=None):
        if isinstance(thing_with_id, int):
            thing_with_id = str(thing_with_id)

        super().__init__(thing_with_id, cache)
        self._errors = []

    def is_dir(self):
        return int(self.id) in self.dirs

    def is_file(self):
        return not self.is_dir()

    def as_path(self):
        return pathlib.PurePosixPath(self.index_at_time[self.test_time][int(self.id)].relative_to(self.anchor))

    @classmethod
    def invAtTime(cls, path, index):
        path = cls.anchor / path
        return str({p:i for i, p in cls.index_at_time[index].items()}[path])

    @property
    def name(self):
        return self.as_path().name

    @property
    def parent(self):
        if int(self.id) == 0:
            return None

        rlu = self.as_path().parent
        return self.__class__(self.invAtTime(rlu, self.test_time))

    @property
    def meta(self):
        return PathMeta(id=self.id)

    def __repr__(self):
        p = self.as_path()
        return f'{self.__class__.__name__} <{self.id!r} {p!r}>'


# set up cache hierarchy
LocalPathTest._cache_class = CachePathTest
CachePathTest._local_class = LocalPathTest
CachePathTest._remote_class = RemotePathTest
RemotePathTest._cache_class = CachePathTest

# set up testing anchor (must come after the hierarchy)
CachePathTest.anchor = test_path
# note: the this creates a symlink which the rests of the tests expect
if test_path.exists():
    test_path.rmtree(onerror=onerror)
CachePathTest.anchor = CachePathTest(test_path, meta=PathMeta(id='0'))
test_path.unlink()


class TestPathHelper:
    @classmethod
    def setUpClass(cls):
        if cls.test_base.exists():
            shutil.rmtree(cls.test_base, onerror=onerror)

        cls.test_base.mkdir()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_base, onerror=onerror)

    def setUp(self, init_cache=True):
        if self.test_path.exists():  # in case something went wrong with a previous test
            shutil.rmtree(self.test_path, onerror=onerror)

        self.test_path.mkdir()
        if init_cache:
            self.test_path.cache_init('0')

    def tearDown(self):
        shutil.rmtree(self.test_path, onerror=onerror)


TestPathHelper.test_base = test_base  # there are here to prevent
TestPathHelper.test_path = test_path  # pytest from discovering them
