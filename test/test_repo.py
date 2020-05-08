import os
import pathlib
import unittest
import pytest
import augpathlib as aug
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
        RepoPath(testing_base).rmtree(onerror=onerror)

    def test_init(self):
        rp = testing_base / 'test-repo'
        repo = rp.init()
        assert repo, f'hrm {rp!r} {repo}'
        return rp

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
        if rp.working_dir is not None:
            pytest.skip('not testing inside another git repo')

        if this_repo_path is None:
            pytest.skip('this test file is not under version control, so there is no local repo')

        else:
            this_repo = this_repo_path.repo
            repo = rp.init(this_repo_path, depth=1)
            assert repo, f'{rp!r} {repo}'
            return rp

    @skipif_no_net
    def test_init_with_remote(self):
        rp = testing_base / 'test-repo'
        repo = rp.init('https://github.com/tgbugs/augpathlib.git', depth=1)
        assert repo, f'{rp!r} {repo}'
        return rp

    @skipif_no_net
    def test_clone_from(self):
        rp = testing_base.clone_from('https://github.com/tgbugs/augpathlib.git', depth=1)
        assert rp.repo, f'{rp!r} {rp.repo}'
        return rp

    def test_show(self):
        test_byte1 = b'\x98'
        test_byte2 = b'\x99'
        tag1 = 'tag1'
        tag2 = 'tag2'
        rp = testing_base / 'test-repo-show'
        rp.init()
        rp.repo.git.commit(message='Inital commit', allow_empty=True)

        tf = rp / 'test-file'
        with open(tf, 'wb') as f:
            f.write(test_byte1)

        tf.commit_from_working_tree("test byte one")
        tf.repo.create_tag(tag1)

        with open(tf, 'wb') as f:
            f.write(test_byte2)

        tf.commit_from_working_tree("test byte two")
        tf.repo.create_tag(tag2)

        value = tf.show('tag1')
        assert value == test_byte1

    def test_stale_repo_cache(self):
        rp = self.test_init()
        rp.repo
        aug.AugmentedPath(rp).rmtree()  # have to call rmtree that won't invoke repo.close()
        try:
            rp.repo
            assert False, 'should have failed'
        except exc.NotInRepoError:
            pass

        rp2 = RepoPath(rp)
        try:
            rp2.repo
            assert False, 'should have failed'
        except exc.NotInRepoError:
            pass

    @skipif_no_net
    def test_remote_uris(self):
        rp = self.test_init_with_remote()
        h = rp.remote_uri_human()
        m = rp.remote_uri_machine()
        h = rp.remote_uri_human('master')
        m = rp.remote_uri_machine('master')
        assert h != m  # TODO probably need a better test here

    def test_repo_context(self):
        rp = testing_base / 'test-repo-context'
        rp.init()
        rp.repo.git.commit(message='Inital commit', allow_empty=True)
        ref_target = 'ref-target'
        rp.repo.create_head(ref_target)

        test_byte1 = b'\x98'
        test_byte2 = b'\x99'
        test_byte3 = b'\x97'
        test_byte4 = b'\x96'

        tc = rp / 'test-committed'
        with open(tc, 'wb') as f:
            f.write(test_byte1)

        tc.commit_from_working_tree("test byte one")

        tu = rp / 'test-uncommitted'
        with open(tu, 'wb') as f:
            f.write(test_byte2)

        with rp.repo.getRef(ref_target):
            # this will cause an error I'm sure ...
            # will have to reorder the stack in this case
            # numerous types of errors we can expect here
            '''
            git.exc.GitCommandError: Cmd('git') failed due to: exit code(1)
              cmdline: git checkout master
              stderr: 'error: The following untracked working tree files would be overwritten by checkout:
            	test-committed
            Please move or remove them before you switch branches.
            Aborting'
            '''

            with open(tc, 'wb') as f:
                f.write(test_byte3)

            with open(tu, 'wb') as f:
                f.write(test_byte4)


class TestComplex(unittest.TestCase):
    test_file = 'test-file'

    def setUp(self):
        self.hp = HybridPath(testing_base).clone_path('test-repo')
        self.hp.init()
        self.test_file = self.hp / self.test_file

    def tearDown(self):
        RepoPath(testing_base).rmtree(onerror=onerror)

    def test_commit(self):
        self.test_file.touch()
        self.test_file.add_index()
        c1 = self.test_file.commit(message='test commit 1')
        self.test_file.data = (b'a' for _ in (0,))
        self.test_file.add_index()
        c2 = self.test_file.commit(message='test commit 1')
        self.test_file.data = (b'b' for _ in (0,))

    def test_diff(self):
        self.test_commit()
        d = self.test_file.diff('HEAD', 'HEAD~1')
        assert d, f'd'

    def test_working_dir(self):
        [RepoPath._repos.pop(k) for k in list(RepoPath._repos)]
        rp = RepoPath(str(self.test_file))
        assert rp.working_dir is not None, f'wat {rp}'
        assert rp.repo is not None, f'wat {rp} {rp.working_dir}'
