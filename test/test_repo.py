import os
import pathlib
import unittest
import pytest
from augpathlib import RepoPath, LocalPath, exceptions as exc
from .common import onerror, skipif_no_net, temp_path

testing_base = RepoPath(temp_path)


class HybridPath(RepoPath, LocalPath):
    """ Combined functionality """


HybridPath._bind_flavours()


class TestRepoPath(unittest.TestCase):
    def setUp(self):
        if not testing_base.exists():
            testing_base.mkdir()

    def tearDown(self):
        LocalPath(testing_base).rmtree(onerror=onerror)

    def test_init(self):
        rp = testing_base / 'test-repo'
        repo = rp.init()
        assert repo, f'hrm {rp!r} {repo}'

    def test_clone_path(self):
        rp = testing_base.clone_path('git@github.com:tgbugs/augpathlib.git')
        assert rp.name == 'augpathlib', 'ssh form failed'

        rp = testing_base.clone_path('https://github.com/tgbugs/augpathlib.git')
        assert rp.name == 'augpathlib', 'https form failed'

        rp = testing_base.clone_path('~/git/augpathlib')
        assert rp.name == 'augpathlib', 'local form failed'

    def test_init_from_local_repo(self):
        rp = testing_base / 'test-repo'
        this_repo_path = RepoPath(__file__).working_dir
        if this_repo_path is not None:
            this_repo = this_repo_path.repo
            repo = rp.init(this_repo_path, depth=1)
            assert repo, f'{rp!r} {repo}'
        else:
            pytest.skip('not testing from inside a git repo')

    @skipif_no_net
    def test_init_with_remote(self):
        rp = testing_base / 'test-repo'
        repo = rp.init('https://github.com/tgbugs/augpathlib.git', depth=1)
        assert repo, f'{rp!r} {repo}'

    @skipif_no_net
    def test_clone_from(self):
        rp = testing_base.clone_from('https://github.com/tgbugs/augpathlib.git', depth=1)
        assert rp.repo, f'{rp!r} {rp.repo}'


class TestComplex(unittest.TestCase):
    test_file = 'test-file'

    def setUp(self):
        self.hp = HybridPath(testing_base).clone_path('test-repo')
        self.hp.init()
        self.test_file = self.hp / self.test_file

    def tearDown(self):
        LocalPath(testing_base).rmtree(onerror=onerror)

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
