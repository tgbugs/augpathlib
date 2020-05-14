import pathlib
from urllib.parse import urlparse
import git
from git import Repo
from augpathlib import AugmentedPath
from augpathlib import exceptions as exc
from augpathlib.utils import log as _log

log = _log.getChild('repo')


class _Repo(git.Repo):  # FIXME should we subclass Repo for this or patch ??
    """ monkey patching """

    def getRef(self, ref_name):
        for ref in self.refs:
            if ref.name == ref_name:
                return ref

        else:
            raise ValueError(f'No ref with name: {ref_name}')


# monkey patch git.Repo
git.Repo.getRef = _Repo.getRef


class _Reference(git.Reference):
    """ monkey patching """
    def __enter__(self):
        """ Checkout the ref for this head.
        `git stash --all` beforehand and restore during __exit__.

        If the ref is the same, then the stash step still happens.
        If you need to modify the uncommitted state of a repo this
        is not the tool you should use. """

        if not self.is_valid():
            raise exc.InvalidRefError(f'Not a valid ref: {self.name}')

        self.__original_branch = self.repo.active_branch
        self.__stash = self.repo.git.stash('--all')  # always stash
        if self.__stash == 'No local changes to save':
            self.__stash = None

        if self == self.__original_branch:
            return self

        self.checkout()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        _stash = self.repo.git.stash('--all')  # always stash on the way out as well
        if _stash == 'No local changes to save':
            stash = 'stash@{0}'
        else:
            stash = "stash@{1}"

        if self.__original_branch != self:
            self.__original_branch.checkout()

        # TODO check to make sure no other stashes were pushed on top
        if self.__stash is not None:
            self.repo.git.stash('pop', stash)

        self.__stash = None


# monkey patch git.Reference
git.Reference.__enter__ = _Reference.__enter__
git.Reference.__exit__ = _Reference.__exit__


class RepoHelper:
    _repo_class = Repo
    _repos = {}  # repo cache

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
                msg = 'how!? {self!r} != {repo.working_dir}'
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

    def _remote_uri(self, prefix, infix=None, ref=None):
        if isinstance(ref, git.Commit):
            ref = str(ref)

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
        else:
            raise NotImplementedError(url_base)

    def remote_uri_human(self, ref=None):
        return self._remote_uri('https://github.com', infix='blob', ref=ref)

    def remote_uri_machine(self, ref=None):
        return self._remote_uri('https://raw.githubusercontent.com', ref=ref)

    def latest_commit(self, rev=None):
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
                except git.exc.GitCommandError:
                    pass

                commit = self.repo.index.commit(message=message,
                                                author_date=date)
                return commit
            finally:
                try:
                    self.repo.git.stash('pop')
                except git.exc.GitCommandError:
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
