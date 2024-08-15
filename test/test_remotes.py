import os
import unittest
from socket import gethostname
from pathlib import PurePath
import pytest
from augpathlib import LocalPath
from augpathlib.caches import SshCache, ReflectiveCache
from augpathlib.remotes import SshRemote
from .common import project_path, TestPathHelper, skipif_no_net

@skipif_no_net
@pytest.mark.skipif(os.name == 'nt', reason='No pxssh for windows at the moment.')
@pytest.mark.skipif('CI' in os.environ, reason='Requires ssh/.config to be set up correctly.')
class TestSshRemote(TestPathHelper, unittest.TestCase):
    def setUp(self):
        super().setUp(init_cache=False)
        class Path(LocalPath):
            pass
        Path._bind_flavours()
        class Cache(SshCache):
            pass
        Cache._bind_flavours()

        self.Path = Path
        self.Cache = Cache
        self.SshRemote = SshRemote._new(self.Path, self.Cache)

        project_path = self.Path(self.test_path)
        self.project_path = project_path
        remote_root = PurePath(self.Path(__file__).parent)  # the 'remote' target
        remote_id = remote_root.as_posix()

        hostname = gethostname()
        assert not hasattr(self.Path._cache_class, '_anchor'), 'oops should not have cache anchor yet'
        # this_folder.meta is sort of one extra level of host keys
        try:
            anchor = project_path.cache_init(hostname + ':' + remote_id, anchor=True)
        except TypeError as e:  # pxssh fail
            log.exception(e)
            anchor = project_path.cache_init(hostname + '-local:' + remote_id, anchor=True)

        assert hasattr(self.Path._cache_class, '_anchor'), 'oops no cache anchor'
        # FIXME remote_root doesn't actually work for ssh remotes, it is always '/'
        #anchor = project_path.cache_init('/')  # this_folder.meta is sort of one extra level of host keys
        self.this_file = self.Path(__file__)
        self.this_file_darkly = self.SshRemote(__file__)
        tfd_cache = self.this_file_darkly.cache_init()

    def test_checksum(self):
        assert self.this_file_darkly.meta.checksum == self.this_file.meta.checksum

    def test_parent(self):
        r = self.this_file_darkly
        l = self.Path(r.local)
        assert hasattr(l._cache_class, '_anchor'), 'oops no cache anchor'
        # FIXME utterly broken on 3.12
        r.parent.local
        r.local.parent
        l.parent
        assert hasattr(l.parent._cache_class, '_anchor'), 'oops no cache anchor'
        l.parent.remote  # the problem in 3.12 happens here
        l.remote.parent
        r.parent
        assert r.parent.local == r.local.parent == l.parent
        assert l.parent.remote == l.remote.parent == r.parent

    def test_meta(self):
        #hrm = this_file_darkly.meta.__dict__, this_file_darkly.local.meta.__dict__
        #assert hrm[0] == hrm[1]
        rm = self.this_file_darkly.meta
        lm = self.Path(__file__).meta

        rmnid = {k:v for k, v in rm.items() if k not in ('id', 'parent_id')}
        lmnid = {k:v for k, v in lm.items() if k not in ('id', 'parent_id')}
        bads = []
        for k, rv in rmnid.items():
            lv = lmnid[k]
            if rv != lv:
                bads.append((lv, rv))

        assert not bads, bads

    def test_data(self):
        # FIXME this_file_darkly.local seems to not be in quite the right location?
        #assert list(self.this_file_darkly.data) == list(self.this_file_darkly.local.data)
        assert list(self.this_file_darkly.data) == list(self.this_file.data)

    #stats, checks = this_file_darkly.parent.children  # FIXME why does this list the home directory!?
    def test_access(self):
        f = self.SshRemote('/root/this-file-does-not-exist')
        assert not f.access('read')
        f = self.SshRemote(__file__)
        assert f.access('write')
