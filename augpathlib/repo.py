import pathlib
from urllib.parse import urlparse
from augpathlib import AugmentedPath
from augpathlib import exceptions as exc
from augpathlib.utils import log as _log

log = _log.getChild('repo')


class RepoHelper:
    _repo_class = None  # set in _setup
    _repos = {}  # repo cache

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls, *args, **kwargs)

    _renew = __new__

    def __new__(cls, *args, **kwargs):
        RepoHelper._setup(*args, **kwargs)
        RepoHelper.__new__ = RepoHelper._renew
        return super().__new__(cls, *args, **kwargs)

    @staticmethod
    def _setup(*args, **kwargs):
        import git
        from git import Repo
        from . import repo_patch
        RepoHelper._git = git
        RepoHelper._repo_class = Repo

    def clone_path(self, remote):
        """ get the path to which a repo would clone
            this makes it possible to check for various issues
            prior to calling init(remote)
        """
        name = pathlib.PurePath(remote).stem
        return self / name

    def clone_from(self, remote, *, depth=None):
        """ clone_from uses the path of the current object as the
            parent path where a new folder will be created with the remote's name

            NOTE: clone_from always uses the remote's naming convention if you want
            to clone into a folder with a different name use init(remote) instead

            NOTE: this does not return the new repo it returns the new
            child path at which the repo is located

            You should probably not use this method since it is poorly designed
            because it requires error handling in the case where a repository
            with the name of the remote has already been cloned as a child of
            the current path
        """
        repo_path = self.clone_path(remote)
        # in a more specific application a variety of tests should go here
        repo_path.init(remote, depth=depth)
        return repo_path

    def init(self, remote=None, depth=None):
        """ NOTE: init conflates init with clone_from
            in cases where a path is known before a remote

            No bare option is provided for init since we assume
            that if you are using this class then you probably
            want the files in the working tree

            NOTE: this does not protect from creating repos
            that contain other repos already, only from creating
            a nested repo inside an existing repo """

        # TODO is_dir() vs is_file()?
        try:
            repo = self.repo
        except exc.NotInRepoError:
            repo = None

        if repo is not None:
            if not self.exists():
                msg = f'how!? {self!r} != {repo.working_dir}'
                assert repo.working_dir == self.as_posix(), msg
                log.warning(f'stale cache on deleted repo {self!r}')
                self._repos.pop(self)
            else:
                raise exc.RepoExistsError(f'{repo}')

        if remote is not None:
            if isinstance(remote, pathlib.Path):
                remote = str(remote)

            repo = self._repo_class.clone_from(remote, self, depth=depth)
        else:
            repo = self._repo_class.init(self)

        self._repos[self] = repo
        return repo

    @property
    def repo(self):
        wd = self.working_dir
        if wd in self._repos:
            return self._repos[wd]
        elif wd is not None:
            repo = self._repo_class(wd.as_posix())
            self._repos[wd] = repo
            return repo
        else:
            raise exc.NotInRepoError(f'{self} is not in a git repository')

    @property
    def working_dir(self):
        # TODO match git behavior here
        # https://github.com/git/git/blob/master/setup.c#L903
        # https://github.com/git/git/blob/08da6496b61341ec45eac36afcc8f94242763468/setup.c#L584
        # https://github.com/git/git/blob/bc12974a897308fd3254cf0cc90319078fe45eea/setup.c#L300
        if (self / '.git').exists():
            if not self.is_absolute():
                # avoid cases where RepoPath('.') gets put in the repos cache
                return self.expanduser().absolute()
            else:
                return self

        elif str(self) == self.anchor:  # anchor is portable
            return None

        else:
            if not self.is_absolute():
                return self.expanduser().absolute().parent.working_dir
            else:
                return self.parent.working_dir

    @property
    def repo_relative_path(self):
        """ working directory relative path """
        repo = self.repo
        if repo is not None:
            if not self.is_absolute():
                path = self.expanduser().absolute()
            else:
                path = self

            return path.relative_to(repo.working_dir)

    path_relative_repo = repo_relative_path

    def _remote_helper(self):
        repo = self.repo
        remote = repo.remote()
        rnprefix = remote.name + '/'
        url_base = next(remote.urls)
        if url_base.startswith('git@'):
            url_base = 'ssh://' + url_base

        pu = urlparse(url_base)
        netloc = pu.netloc
        path = pu.path
        if netloc.startswith('git@github.com'):
            _, group = netloc.split(':')
            netloc = 'github.com'
            path = '/' + group + path

        return repo, rnprefix, url_base, netloc, path

    def _remote_uri(self, prefix, infix=None, ref=None):
        if isinstance(ref, self._git.Commit):
            ref = str(ref)

        repo, rnprefix, url_base, netloc, path = self._remote_helper()

        if netloc == 'github.com':

            if not ref or ref == 'HEAD':
                ref = repo.active_branch.name
            elif (ref not in [r.name.replace(rnprefix, '') for r in repo.refs] and
                  ref not in [c.hexsha for c in repo.iter_commits(ref, max_count=1)]):
                log.warning(f'unknown ref {ref}')

            if infix is not None:
                rpath = pathlib.PurePosixPath(path).with_suffix('') / infix / ref / self.repo_relative_path
            else:
                rpath = pathlib.PurePosixPath(path).with_suffix('') / ref / self.repo_relative_path
            return prefix + rpath.as_posix()
        elif url_base.startswith('/') and self.__class__(url_base).exists():
            other_rp = self.__class__(url_base)
            other = other_rp / self.repo_relative_path
            return other._remote_uri(prefix, infix, ref)
        else:
            raise NotImplementedError(url_base)

    def remote_uri_api(self, endpoint=''):
        # technically this is for the whole repo
        repo, rnprefix, url_base, netloc, path = self._remote_helper()
        p = pathlib.PurePosixPath(path).with_suffix('')
        return f'https://api.github.com/repos{p}' + endpoint

    def remote_uri_human(self, ref=None):
        return self._remote_uri('https://github.com', infix='blob', ref=ref)

    def remote_uri_machine(self, ref=None):
        return self._remote_uri('https://raw.githubusercontent.com', ref=ref)

    def latest_commit(self, rev=None):
        """ the latest commit for the current path NOT the latest commit for the repo """
        try:
            return next(self.repo.iter_commits(rev=rev,
                                               paths=self.expanduser().resolve().as_posix(),
                                               max_count=1))
        except StopIteration as e:
            raise exc.NoCommitsForFile(self) from e

    def commits(self, *, rev=None, max_count=None):
        yield from self.repo.iter_commits(
            rev=rev,
            paths=self.expanduser().resolve().as_posix(),
            # TODO --follow
            max_count=max_count)

    # a variety of change detection

    def modified(self):
        """ has the filed been changed against index or HEAD """
        return self._do_diff(self.repo.index, None)
        #return self.diff()

    def indexed(self):
        """ cached, or in the index, something like that """
        return self._do_diff(self.repo.head.commit, self.repo.index)
        #return self.diff('HEAD', '')

    def has_uncommitted_changes(self):
        """ indexed or modified aka test working tree against HEAD """
        return self._do_diff(self.repo.head.commit, None)
        #return self.diff('HEAD')

    def _do_diff(self, this, other, *, create_patch=False):
        """ note that the order is inverted from self.diff """
        if not self.exists():
            raise FileNotFoundError(f'{self}')

        list_ = this.diff(other=other, paths=self.repo_relative_path.as_posix(), create_patch=create_patch)
        if list_:
            return list_[0]

    def diff(self, ref='', ref_orig=None, create_patch=False):
        """ ref can be HEAD, branch, commit hash, etc.

            default behaviors diffs the working tree against the index or HEAD if no index

            if ref = None, diff against the working tree
            if ref = '',   diff against the index

            if ref_orig = None, diff from the working tree
            if ref_orig = '',   diff from the index
        """

        def ref_to_object(ref_):
            if ref_ is None:
                return None
            elif ref_ == '':
                return self.repo.index
            else:
                return self.repo.commit(ref_)

        this = ref_to_object(ref_orig)
        other = ref_to_object(ref)

        if this is None:
            if other is None:
                return  # FIXME align return type
            else:
                this, other = other, this

        diff = self._do_diff(this, other, create_patch=create_patch)
        # TODO None -> '' I think?
        # TODO do we render this here or as an extension to the diff?
        return diff

    # commit this file

    def add_index(self):
        """ git add -- {self} """
        # FIXME workaround for broken GitPython behavior on windows
        # C:\\Users\\tom\\git\\repo-name != C:/Users/tom/git/repo-name
        # because they are testing with strings
        # so we use repo_relative_path to avoid the broken
        # _to_relative_path in GitPython
        self.repo.index.add([self.repo_relative_path.as_posix()])

    def commit(self, message, *, date=None):
        """ commit from index
            git commit -m {message} --date {date} -- {self}
        """
        # TODO
        # use a modified Index.write_tree create an in memory tree
        # filtering out changed files that are not the current file
        # during the call to mdb.stream_copy, though it seems like
        # the internal call to write_tree_from_cache may be writing
        # all changes and calculating the sha from that so it may
        # make more sense to try to filter entries instead ...
        # but that means a blob may still be sitting there and
        # get incorporated? I may have to use the full list of entries
        # but sneekily swap out the entries for other changed files for
        # the unmodified entry for their object, will need to experiment


        if self.repo.active_branch.is_valid(): # handle the empty repo case
            # HAH take that complexity!
            _mes = self.repo.git.stash('push')
            try:
                # FIXME concurrent modification?!
                try:
                    self.repo.git.checkout('stash@{0}', '--', self.as_posix())
                except self._git.exc.GitCommandError:
                    pass

                commit = self.repo.index.commit(message=message,
                                                author_date=date)
                return commit
            finally:
                try:
                    self.repo.git.stash('pop')
                except self._git.exc.GitCommandError:
                    pass
        else:
            try:
                self.repo.tree()
                raise ValueError('we are not in the situation we though we were in')
            except:
                commit = self.repo.index.commit(message=message,
                                                author_date=date)
                return commit

    def commit_from_working_tree(self, message, *, date=None):
        """ commit from working tree by automatically adding to index
            git add -- {self}
            git commit -m {message} --date {date} -- {self}
        """
        self.add_index()
        return self.commit(message, date=date)

    # show version file

    def show(self, ref=''):
        # TODO make sure ref='' -> index

        if ref is None:
            with open(self, 'rb') as f:
                return f.read()
        else:
            decoded = self.repo.git.show(ref + ':' + self.repo_relative_path.as_posix())
            return decoded.encode('utf-8', 'surrogateescape')

        return  # unfortunately traversing the tree is a pain for this
        def ref_to_object(ref_):
            if ref_ is None:
                return None
            elif ref_ == '':
                return self.repo.index
            else:
                return self.repo.commit(ref_)

        this = ref_to_object(ref)

        if this is None:
            with open(self, 'rb') as f:
                return f.read()
        else:
            p = self.repo_relative_path.as_posix()
            for blob in this.tree.blobs:
                if blob.path == p:
                    ds = blob.data_stream
                    fcs = ds[-1]
                    return fcs.read()


class RepoPath(RepoHelper, AugmentedPath):
    def rmtree(self, ignore_errors=False, onerror=None, DANGERZONE=False):
        if self in self._repos:
            # remove the reference to the soon to be stale repo
            # in order to prevent GitPython cmd.Git._get_persistent_cmd
            # from causing errors on windows
            # https://github.com/gitpython-developers/GitPython/issues/553
            # https://github.com/gitpython-developers/GitPython/pull/686
            self._repos.pop(self).close()

        super().rmtree(ignore_errors=ignore_errors,
                       onerror=onerror,
                       DANGERZONE=DANGERZONE)


RepoPath._bind_flavours()
