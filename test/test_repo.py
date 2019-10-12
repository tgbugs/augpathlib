import os
import pathlib
import unittest
from augpathlib import RepoPath, LocalPath
import pytest

testing_base = RepoPath(f'/tmp/.augpathlib-testing-base-{os.getpid()}')


class HybridPath(RepoPath, LocalPath):
    """ Combined functionality """


class HWP(HybridPath, pathlib.WindowsPath): pass
class HPP(HybridPath, pathlib.PosixPath): pass
HybridPath._bind_flavours()


class TestRepoPath(unittest.TestCase):
    def setUp(self):
        testing_base.mkdir()

    def tearDown(self):
        LocalPath(testing_base).rmtree()

    def test_init(self):
        rp = testing_base / 'test-repo'
        repo = rp.init()
        assert repo, f'hrm {rp!r} {repo}'

    def test_clone_path(self):
        rp = testing_base.clone_path('git@github.com:tgbugs/augpathlib.git')
        assert rp.name == 'augpathlib', 'ssh form failed'

        rp = testing_base.clone_path('https://github.com/tgbugs/augpathlib.git')
        assert rp.name == 'augpathlib', 'https form failed'

    def test_init_with_remote(self):
        # XXX hits the network
        rp = testing_base / 'test-repo'
        repo = rp.init('https://github.com/tgbugs/augpathlib.git', depth=1)
        assert repo, f'{rp!r} {repo}'

    def test_clone_from(self):
        # XXX hits the network
        rp = testing_base.clone_from('https://github.com/tgbugs/augpathlib.git', depth=1)
        assert rp.repo, f'{rp!r} {rp.repo}'


class TestComplex(unittest.TestCase):
    test_file = 'test-file'

    def setUp(self):
        self.hp = HybridPath(testing_base).clone_path('test-repo')
        self.hp.init()
        self.test_file = self.hp / self.test_file

    def tearDown(self):
        LocalPath(testing_base).rmtree()

    @pytest.mark.skip('TODO, commit not working yet')
    def test_commit(self):
        return
        self.test_file.touch()
        self.test_file.add_index()
        c1 = self.test_file.commit(message='test commit 1')
        self.test_file.data = (b'a' for _ in (0,))
        self.test_file.add_index()
        c2 = self.test_file.commit(message='test commit 1')
        self.test_file.data = (b'b' for _ in (0,))

    @pytest.mark.skip('TODO')
    def test_diff(self):
        self.test_commit()
        d = self.test_file.diff('HEAD', 'HEAD~1')
        assert d, f'd'

    def test_working_dir(self):
        [RepoPath._repos.pop(k) for k in list(RepoPath._repos)]
        rp = RepoPath(str(self.test_file))
        assert rp.working_dir is not None, f'wat {rp}'
        assert rp.repo is not None, f'wat {rp} {rp.working_dir}'
