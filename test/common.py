import shutil
from pathlib import PurePosixPath
from augpathlib import exceptions as exc
from augpathlib import LocalPath, PrimaryCache, RemotePath
from augpathlib import XattrCache, SymlinkCache
from augpathlib import PathMeta

this_file = LocalPath(__file__)
project_path = this_file.parent / 'test_local/test_project'


class TestLocalPath(LocalPath):
    def metaAtTime(self, time):
        # we are cheating in order to do this
        return PathMeta(id=self._cache_class._remote_class.invAtTime(self, time))


test_base = TestLocalPath(__file__).parent / 'test-base'
test_path = test_base / 'test-container'


class TestCachePath(PrimaryCache, XattrCache):
    xattr_prefix = 'test'
    #_backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache


class TestRemotePath(RemotePath):
    anchor = test_path
    ids = {0: anchor}  # time invariant
    dirs = {2, 3, 4, 8, 9, 11, 12, 13, 14, 16, 17, 18}
    index_at_time = {1: {1: anchor / 'a',

                         2: anchor / 'c',

                         3: anchor / 'e',
                         4: anchor / 'e/f',
                         5: anchor / 'e/f/g',

                         8: anchor / 'i',
                         9: anchor / 'i/j',
                         10: anchor / 'i/j/k',

                         13: anchor / 'n',
                         14: anchor / 'n/o',
                         15: anchor / 'n/o/p',

                         18: anchor / 't',},
                     2: {1: anchor / 'b',

                         2: anchor / 'd',

                         3: anchor / 'h/',
                         4: anchor / 'h/f',
                         5: anchor / 'h/f/g',

                         11: anchor / 'l',
                         12: anchor / 'l/m',
                         10: anchor / 'l/m/k',

                         16: anchor / 'q',
                         17: anchor / 'q/r',
                         15: anchor / 'q/r/s',

                         18: anchor / 't',

                         19: anchor / 'u',}}

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
        return PurePosixPath(self.index_at_time[self.test_time][int(self.id)].relative_to(self.anchor))

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
TestLocalPath._cache_class = TestCachePath
TestCachePath._local_class = TestLocalPath
TestCachePath._remote_class = TestRemotePath
TestRemotePath._cache_class = TestCachePath

# set up testing anchor (must come after the hierarchy)
TestCachePath.anchor = test_path
TestCachePath.anchor = TestCachePath(test_path, meta=PathMeta(id='0'))


class TestPathHelper:
    @classmethod
    def setUpClass(cls):
        if cls.test_base.exists():
            shutil.rmtree(cls.test_base)

        cls.test_base.mkdir()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_base)

    def setUp(self, init_cache=True):
        if self.test_path.exists():  # in case something went wrong with a previous test
            shutil.rmtree(self.test_path)

        self.test_path.mkdir()
        if init_cache:
            self.test_path.cache_init('0')

    def tearDown(self):
        shutil.rmtree(self.test_path)


TestPathHelper.test_base = test_base
TestPathHelper.test_path = test_path
