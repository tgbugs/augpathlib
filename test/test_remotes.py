import os
import unittest
from socket import gethostname
from pathlib import PurePath
import pytest
from augpathlib import LocalPath as Path
from augpathlib.caches import SshCache, ReflectiveCache
from augpathlib.remotes import SshRemoteFactory
from .common import project_path, TestPathHelper, skipif_no_net

@skipif_no_net
@pytest.mark.skipif(os.name == 'nt', reason='No pxssh for windows at the moment.')
@pytest.mark.skipif('CI' in os.environ, reason='Requires ssh/.config to be set up correctly.')
class TestSshRemote(TestPathHelper, unittest.TestCase):
    def setUp(self):
        super().setUp(init_cache=False)
        hostname = gethostname()
        SshCache._local_class = Path
        Path.setup(SshCache, SshRemoteFactory)  # if this doesn't break something I will be surprised
        project_path = Path(self.test_path)
        self.project_path = project_path
        remote_root = PurePath(Path(__file__).parent)  # the 'remote' target
        remote_id = remote_root.as_posix()
        anchor = project_path.cache_init(remote_id, anchor=True)  # this_folder.meta is sort of one extra level of host keys
        # FIXME remote_root doesn't actually work for ssh remotes, it is always '/'
        #anchor = project_path.cache_init('/')  # this_folder.meta is sort of one extra level of host keys
        try:
            self.SshRemote = SshRemoteFactory(anchor, Path, hostname)
        except TypeError:  # pxssh fail
            self.SshRemote = SshRemoteFactory(anchor, Path, hostname + '-local')
        self.this_file = Path(__file__)
        self.this_file_darkly = self.SshRemote(__file__)
        tfd_cache = self.this_file_darkly.cache_init()

    def test_checksum(self):
        assert self.this_file_darkly.meta.checksum == self.this_file.meta.checksum

    def test_meta(self):
        #hrm = this_file_darkly.meta.__dict__, this_file_darkly.local.meta.__dict__
        #assert hrm[0] == hrm[1]
        rm = self.this_file_darkly.meta
        lm = Path(__file__).meta

        rmnid = {k:v for k, v in rm.items() if k != 'id'}
        lmnid = {k:v for k, v in lm.items() if k != 'id'}
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
