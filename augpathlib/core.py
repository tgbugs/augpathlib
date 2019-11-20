import os
import sys
import shutil
import pathlib
import mimetypes
import subprocess
from time import sleep
from errno import ELOOP, ENOENT, ENOTDIR, EBADF
from urllib.parse import urlparse
from datetime import datetime, timezone
from functools import wraps
from itertools import chain
try:
    import magic  # from sys-apps/file consider python-magic ?
except ImportError:
    pass

#import psutil  # import for experimental xopen functionality
from git import Repo
from dateutil import parser
#from Xlib.display import Display
#from Xlib import Xatom
from augpathlib import exceptions as exc
from augpathlib.meta import PathMeta
from augpathlib.utils import log, default_cypher, StatResult, etag
from augpathlib.utils import _bind_sysid_

_IGNORED_ERROS = (ENOENT, ENOTDIR, EBADF, ELOOP)
_IGNORED_WINERRORS = (
    123,  # 'The filename, directory name, or volume label syntax is incorrect' -> 22 EINVAL
)


if os.name != 'nt':
    import xattr
    XATTR_DEFAULT_NS = xattr.NS_USER
    def _ignore_error(exception):
        return (getattr(exception, 'errno', None) in _IGNORED_ERROS)

else:
    import winreg
    from augpathlib import pyads
    XATTR_DEFAULT_NS = 'user'
    def _ignore_error(exception):
        return ((getattr(exception, 'winerror', None) in _IGNORED_WINERRORS) or
                (getattr(exception, 'errno', None) in _IGNORED_ERROS))


if sys.version_info >= (3, 7):
    pathlib._IGNORED_ERROS += (ELOOP,)

else:

    def _is_dir(entry):
        try:
            return entry.is_dir()
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    def _iterate_directories(self, parent_path, is_dir, scandir):
        """ patched to fix is_dir() erron """
        yield parent_path
        try:
            entries = list(scandir(parent_path))
            for entry in entries:
                if _is_dir(entry) and not entry.is_symlink():
                    path = parent_path._make_child_relpath(entry.name)
                    for p in self._iterate_directories(path, is_dir, scandir):
                        yield p
        except PermissionError:
            return

    pathlib._RecursiveWildcardSelector._iterate_directories = _iterate_directories


def _catch_wrapper(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    return staticmethod(wrapped)


#pathlib._NormalAccessor.stat = _catch_wrapper(os.stat)  # can't wrap stat, pathlib needs the errors

# pathlib helpers, simplify the inheritance nightmare ...


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
                return self.absolute()
            else:
                return self

        elif str(self) == self.anchor:  # anchor is portable
            return None

        else:
            if not self.is_absolute():
                return self.absolute().parent.working_dir
            else:
                return self.parent.working_dir

    @property
    def repo_relative_path(self):
        """ working directory relative path """
        repo = self.repo
        if repo is not None:
            if not self.is_absolute():
                path = self.absolute()
            else:
                path = self

            return path.relative_to(repo.working_dir)

    def _remote_uri(self, prefix, infix=None, ref=None):
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

    @property
    def latest_commit(self):
        try:
            return next(self.repo.iter_commits(paths=self.as_posix(), max_count=1))
        except StopIteration as e:
            raise exc.NoCommitsForFile(self) from e

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
        self.repo.index.add([self.as_posix()])

    def commit(self, message, *, date=None):
        """ commit from index
            git commit -m {message} --date {date} -- {self}
        """
        raise NotImplementedError()
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
        commit = self.repo.index.commit
        breakpoint()
        return commit

    def commit_from_working_tree(self, message, *, date=None):
        """ commit from working tree by automatically adding to index
            git add -- {self}
            git commit -m {message} --date {date} -- {self}
        """
        self.index()
        return self.commit(message, date=date)


class EatHelper:
    """ Extended attributes helper """

    @classmethod
    def _bind_flavours(cls, pos_helpers=tuple(), win_helpers=tuple()):
        pos_helpers = pos_helpers + (XattrHelper,)
        win_helpers = win_helpers + (ADSHelper,)
        super()._bind_flavours(pos_helpers, win_helpers)


class ADSHelper(EatHelper):
    """ Windows NTFS equivalent of Xattrs is Alternate Data Streams
        This class allows ADS to pretend to work like xattrs.
    """

    @staticmethod
    def _key_convention(key, namespace):
        return namespace + '.' + key  # FIXME maybe include xattrs. as well ??

    def _stream(self, name):
        *start, last = self.parts
        if not start and len(last) == 1:
            # single letter file names with no extension
            # masquerade as drive letters on windows and
            # there seems to be nothing we can do about it
            raise ValueError('windows a single letter file names dont get a long')

        return AugmentedPath(*start, last + ':' + name)

    @property
    def _streamname(self):
        *start, last = self.parts
        if ':' in last:
            return last.split(1, ':')[-1]

    @property
    def _streams(self):
        file_infos = pyads.WIN32_FIND_STREAM_DATA()
        #streamlist = list()

        findFirstStreamW = pyads.kernel32.FindFirstStreamW
        findFirstStreamW.restype = pyads.c_void_p

        myhandler = pyads.kernel32.FindFirstStreamW (pyads.LPSTR(self.as_posix()),
                                                     0,
                                                     pyads.byref(file_infos),
                                                     0)
        '''
        HANDLE WINAPI FindFirstStreamW(
        __in        LPCWSTR lpFileName,
        __in        STREAM_INFO_LEVELS InfoLevel, (0 standard, 1 max infos)
        __out       LPVOID lpFindStreamData, (return information about file in a WIN32_FIND_STREAM_DATA if 0 is given in infos_level
        __reserved  DWORD dwFlags (Reserved for future use. This parameter must be zero.) cf: doc
        );
        https://msdn.microsoft.com/en-us/library/aa364424(v=vs.85).aspx
        '''
        try:
            p = pyads.c_void_p(myhandler)

            if file_infos.cStreamName:
                streampath = file_infos.cStreamName
                if '::$DATA' not in streampath:
                    cleaned, _data_suffix = streampath.strip(':').rsplit(':$', 1)
                    yield self._stream(cleaned)

                while pyads.kernel32.FindNextStreamW(p, pyads.byref(file_infos)):
                    streampath = file_infos.cStreamName
                    if '::$DATA' not in streampath:
                        cleaned, _data_suffix = streampath.strip(':').rsplit(':$', 1)
                        yield self._stream(cleaned)

        finally:
            pyads.kernel32.FindClose(p)  # Close the handle

    def setxattr(self, key, value, namespace=XATTR_DEFAULT_NS):
        if not isinstance(value, bytes):  # checksums
            raise TypeError(f'setxattr only accepts values already encoded to bytes!\n{value!r}')
        else:
            bytes_value = value

        if isinstance(key, bytes):
            key = key.decode()

        name = self._key_convention(key, namespace)
        stream = self._stream(name)
        log.debug(name)
        log.debug(stream)
        log.debug(bytes_value)
        with open(stream, 'wb') as f:
            f.write(bytes_value)

    def setxattrs(self, xattr_dict, namespace=XATTR_DEFAULT_NS):
        for k, v in xattr_dict.items():
            self.setxattr(k, v, namespace=namespace)

    def getxattr(self, key, namespace=XATTR_DEFAULT_NS):
        # we don't deal with types here, we just act as a dumb store
        name = self._key_convention(key, namespace)
        with open(self._stream(name), 'rb') as f:
            return f.read()

    def _xattrs(self):
        out = {}
        for stream in self._streams:
            _base, k = stream.name.split(':', 1)
            with open(stream, 'rb') as f:
                v = f.read()

            out[k] = v

        return out

    def xattrs(self, namespace=XATTR_DEFAULT_NS):
        # decode keys later
        ns_length_p1 = len(namespace) + 1
        try:
            # we encode here to match the behavior of the posix version
            return {k[ns_length_p1:].encode():v for k, v in self._xattrs().items()
                    if k.startswith(namespace)}
        except FileNotFoundError as e:
            raise FileNotFoundError(self) from e


class XattrHelper(EatHelper):
    """ pathlib helper augmented with xattr support """

    def setxattr(self, key, value, namespace=XATTR_DEFAULT_NS):
        if not isinstance(value, bytes):  # checksums
            raise TypeError(f'setxattr only accepts values already encoded to bytes!\n{value!r}')
        else:
            bytes_value = value

        xattr.set(self.as_posix(), key, bytes_value, namespace=namespace)

    def setxattrs(self, xattr_dict, namespace=XATTR_DEFAULT_NS):
        for k, v in xattr_dict.items():
            self.setxattr(k, v, namespace=namespace)

    def getxattr(self, key, namespace=XATTR_DEFAULT_NS):
        # we don't deal with types here, we just act as a dumb store
        return xattr.get(self.as_posix(), key, namespace=namespace)

    def xattrs(self, namespace=XATTR_DEFAULT_NS):
        # decode keys later
        try:
            return {k:v for k, v in xattr.get_all(self.as_posix(), namespace=namespace)}
        except FileNotFoundError as e:
            raise FileNotFoundError(self) from e


# remote data about remote objects -> remote_meta
# local data about remote objects -> cache_meta
# local data about local objects -> meta
# remote data about local objects <- not relevant yet? or is this actually rr?

# TODO by convetion we could store 'children' of files in a .filename.ext folder ...
# obviously this doesn't always work
# and we would only want to do this for files that actually had annotations that
# needed a place to live ...


class AugmentedPath(pathlib.Path):
    """ extra conveniences, mostly things that are fixed in 3.7 using IGNORE_ERROS """

    _stack = []  # pushd and popd
    count = 0
    _debug = False  # sigh

    @classmethod
    def _bind_flavours(cls, pos_helpers=tuple(), win_helpers=tuple()):
        pos, win = cls._get_flavours()

        if pos is None:
            pos = type(f'{cls.__name__}Posix',
                       (*pos_helpers, cls, AugmentedPathPosix), {})

        if win is None:
            win = type(f'{cls.__name__}Windows',
                       (*win_helpers, cls, AugmentedPathWindows), {})

        cls.__abstractpath = cls
        cls.__posixpath = pos
        cls.__windowspath = win

    @classmethod
    def _get_flavours(cls):
        pos, win = None, None
        for subcls in cls.__subclasses__():  # direct only
            if subcls._flavour is pathlib._posix_flavour:
                pos = subcls
            elif subcls._flavour is pathlib._windows_flavour:
                win = subcls
            else:
                raise TypeError(f'unknown flavour for {cls} {cls._flavour}')

        return pos, win

    @classmethod
    def _abstract_class(cls):
        return cls.__abstractpath

    def __new__(cls, *args, **kwargs):
        if cls is cls.__abstractpath:
            cls = cls.__windowspath if os.name == 'nt' else cls.__posixpath
        self = cls._from_parts(args, init=False)
        if not self._flavour.is_supported:
            raise NotImplementedError("cannot instantiate %r on your system"
                                      % (cls.__name__,))
        self._init()
        return self

    def exists(self):
        """ Turns out that python doesn't know how to stat symlinks that point
            to their own children, which is fine because that is what we do
            so a reasonable way to short circuit the issue """
        try:
            return super().exists()
        except OSError as e:
            #log.error(e)   # too noisy ... though it reaveals we call exists a lot
            if not _ignore_error(e):
                raise

            return False

    def is_file(self):
        try:
            return super().is_file()
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    def is_dir(self):
        try:
            return super().is_dir()
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    def is_symlink(self):
        try:
            return super().is_symlink()
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    def is_broken_symlink(self):
        """ The prime indicator that we have landed on a symlink that is being
            used to store data. The fullest indicator is the symlink loop if we
            want to implement a special checker that exploits errno.ELOOP 40. """
        return self.is_symlink() and not self.exists()

    def exists_not_symlink(self):
        return self.exists() and not self.is_symlink()

    def resolve(self):
        try:
            return super().resolve()
        except RuntimeError as e:
            msg = ('Unless this call to resolve was a mistake you should switch '
                   'to using readlink instead. Uncomment raise e to get a trace.\n'
                   'Alternately you might want to use absolute() in this situation instead?')
            raise RuntimeError(msg) from e

    def readlink(self, raw=False):
        """ this returns the string of the link only due to cycle issues """
        link = os.readlink(self)
        if isinstance(link, bytes):  # on pypy3 readlink still returns bytes
            link = link.decode()

        if raw:
            return link

        return pathlib.PurePath(link)

    def access(self, mode='read', follow_symlinks=True):
        """ types are 'read', 'write', and 'execute' """
        if mode in (os.R_OK, os.W_OK, os.X_OK):
            pass
        elif mode == 'read':
            mode = os.R_OK
        elif mode == 'write':
            mode = os.W_OK
        elif mode == 'execute':
            mode = os.X_OK
        else:
            raise TypeError(f'Unknown mode {mode}')

        # FIXME pypy3 stuck on 3.5 behavior
        return os.access(self.as_posix(), mode, follow_symlinks=follow_symlinks)

    def commonpath(self, other):
        return self.__class__(os.path.commonpath((self, other)))

    def rename(self, target):
        os.rename(self, target)

    def rmtree(self, ignore_errors=False, onerror=None, DANGERZONE=False):
        """ DANGER ZONE """
        if not self.is_absolute():
            raise exc.WillNotRemovePathError(f'Only absolute paths can be removed recursively. {self}')

        if not (DANGERZONE is True):  # prevent python type coersion
            # TODO test in a chroot
            lenparts = len(self.parts)
            if lenparts <= 2:
                raise exc.WillNotRemovePathError(f'Will not remove top level paths. {self}')
            elif lenparts <= 3 and 'home' in self.parts:
                raise exc.WillNotRemovePathError(f'Will not remove home directories. {self}')
            elif self == self.cwd():
                raise exc.WillNotRemovePathError(f'Will not remove current working directory. {self}')

        try:
            if self.is_dir():
                for path in self.iterdir():
                    path.rmtree(ignore_errors=ignore_errors,
                                onerror=onerror,
                                DANGERZONE=DANGERZONE)

                path = self
                self.rmdir()
            else:
                path = self
                self.unlink()

        except exc.WillNotRemovePathError:
            raise

        except BaseException as e:
            if not ignore_errors:
                if onerror is not None:
                    ftype = 'rmdir' if path.is_dir() else 'unlink'
                    func = getattr(path, ftype)
                    onerror(func, path, sys.exc_info())
                else:
                    raise e

    def chdir(self):
        os.chdir(self)

    def pushd(self):
        if self.is_dir():
            AugmentedPath._stack.append(self.cwd())
            self.chdir()
            print(*reversed(AugmentedPath._stack), self.cwd())
        else:
            raise NotADirectoryError(f'{self} is not a directory')

    @staticmethod
    def popd(N=0, n=False):
        """ see popd --help """
        # note that python lists append in the oppsite direction
        # so we invert the N dex
        reversed_index = - (N + 1)
        if AugmentedPath._stack:
            path = AugmentedPath._stack.pop(reversed_index)
            path.chdir()
            print(*reversed(AugmentedPath._stack), AugmentedPath.cwd())
            return path
        else:
            log.warning('popd: directory stack empty')

    def __enter__(self):
        if self.is_dir():
            self._entered_from = self.cwd()
            self.chdir()
            return self
        else:
            super().__enter__()

    def __exit__(self, t, v, tb):
        if hasattr(self, '_entered_from'):
            # if is_dir fails because of a change still try to return
            self._entered_from.chdir()
        else:
            super().__exit__(t, v, tb)

    @property
    def mimetype(self):
        mime, encoding = mimetypes.guess_type(self.as_uri())
        if mime:
            return mime

    @property
    def encoding(self):
        mime, encoding = mimetypes.guess_type(self.as_uri())
        if encoding:
            return encoding

    @property
    def _magic_mimetype(self):
        """ This can be slow because it has to open the files. """
        if self.exists():
            if hasattr(magic, 'detect_from_filename'):
                # sys-apps/file python-magic api
                return magic.detect_from_filename(self).mime_type
            else:
                # python-magic
                return magic.from_file(self.as_posix(), mime=True)

    def checksum(self, cypher=default_cypher):
        """ checksum() always recomputes from the data
            meta.checksum is static for cache and remote IF it exists """

        if self.is_file():
            if ((hasattr(self, '_cache_class') and
                 hasattr(self._cache_class, 'cypher') and
                 self._cache_class.cypher != cypher)):  # FIXME this could be static ...
                cypher = self._cache_class.cypher

            elif (hasattr(self, 'cypher') and
                  self.cypher != cypher):
                cypher = self.cypher

            m = cypher()
            for chunk in self.data:
                m.update(chunk)

            return m.digest()

    def copy_to(self, target, force=False):
        """ copy from a the current path object to a target path """
        if not target.exists() and not target.is_symlink() or force:
            shutil.copy2(self, target)

        else:
            raise exc.PathExistsError(f'{target}')

    def copy_from(self, source, force=False, copy_cache_meta=False):
        """ copy from a source path to the current path object """
        source.copy_to(self, force=force)


class AugmentedPathPosix(AugmentedPath, pathlib.PosixPath): pass
class AugmentedPathWindows(AugmentedPath, pathlib.WindowsPath):
    _registry_drives = 'hklm', 'hkcu', 'HKLM', 'HKCU'

    if sys.version_info < (3, 8):
        # https://bugs.python.org/issue34384
        def readlink(self, raw=False):
            """ this returns the string of the link only due to cycle issues """
            link = os.readlink(str(self))
            if isinstance(link, bytes):  # on pypy3 readlink still returns bytes
                link = link.decode()

            if raw:
                return link

            return pathlib.PurePath(link)


def splitroot(self, part, sep='\\'):
    first = part[0:1]
    second = part[1:2]
    if (second == sep and first == sep):
        # XXX extended paths should also disable the collapsing of "."
        # components (according to MSDN docs).
        prefix, part = self._split_extended_path(part)
        first = part[0:1]
        second = part[1:2]
    else:
        prefix = ''
    third = part[2:3]
    if (second == sep and first == sep and third != sep):
        # is a UNC path:
        # vvvvvvvvvvvvvvvvvvvvv root
        # \\machine\mountpoint\directory\etc\...
        #            directory ^^^^^^^^^^^^^^
        index = part.find(sep, 2)
        if index != -1:
            index2 = part.find(sep, index + 1)
            # a UNC path can't have two slashes in a row
            # (after the initial two)
            if index2 != index + 1:
                if index2 == -1:
                    index2 = len(part)
                if prefix:
                    return prefix + part[1:index2], sep, part[index2+1:]
                else:
                    return part[:index2], sep, part[index2+1:]
    drv = root = ''
    if second == ':' and first in self.drive_letters:
        drv = part[:2]
        part = part[2:]
        first = third
    else:
        index1 = part.find(':')
        index2 = part.find(sep)
        if index1 != -1 and (index2 == -1 or index1 < index2):
            maybe_drv = part[:index1]
            if maybe_drv in self.drive_letters:
                drv = part[:index1]
                part = part[index1:]
                first = part[0:1]

    if first == sep:
        root = first
        part = part.lstrip(sep)
    return prefix + drv, root, part


pathlib._WindowsFlavour.drive_letters.update(AugmentedPathWindows._registry_drives)
pathlib._WindowsFlavour.splitroot = splitroot
pathlib._windows_flavour.splitroot = pathlib._WindowsFlavour().splitroot

AugmentedPath._bind_flavours()


class LocalPath(EatHelper, AugmentedPath):
    # local data about remote objects

    chunksize = 4096  # make the data generator chunksize visible externally
    _cache_class = None  # must be defined by child classes
    sysid = None  # set below

    _bind_sysid = classmethod(_bind_sysid_)

    @classmethod
    def setup(cls, cache_class, remote_class_factory):
        """ call this once to bind everything together """
        cache_class.setup(cls, remote_class_factory)

    @property
    def remote(self):
        return self.cache.remote

    @property
    def cache(self):
        # local can't make a cache because id doesn't know the remote id
        # but if there is an existing cache (duh) the it can try to get it
        # otherwise it will error (correctly)
        if not hasattr(self, '_cache'):
            try:
                self._cache_class(self)  # we don't have to assign here because cache does it
            except exc.NoCachedMetadataError as e:
                #log.error(e)
                return None
            except TypeError as e:
                if self._cache_class is None:
                    return None

        return self._cache

    def cache_init(self, id_or_meta, anchor=False):
        """ wow it took way too long to realize this was the way to do it >_< """
        if self.cache and self.cache.meta:
            raise ValueError(f'Cache already exists! {self.cache}\n'
                             f'{self.cache.meta}')

        elif not self.exists():
            raise ValueError(f'Cannot init a cache on a non-existent path!\n{self}')
        #elif not self.is_dir():
            #raise ValueError(f'Can only init a cache on a directory!\n{self}')

        if not isinstance(id_or_meta, PathMeta):
            id_or_meta = PathMeta(id=id_or_meta)

        cache = self._cache_class(self, meta=id_or_meta)
        if anchor:
            cache.anchorClassHere()

        return cache

    def mkdir_cache(self, remote):
        """ wow side effects everywhere """
        cc = self._cache_class
        rc = cc._remote_class
        for parent in reversed(tuple(remote.parents)):
            # remote as_path is always a PurePosixPath relative to the
            # anchor and does not include the anchor
            local_path = cc.anchor.local / parent.as_path()
            if not local_path.exists():
                local_path.mkdir()
                rc(parent, cache=cc(local_path, remote=parent, meta=parent.meta))

    def find_cache_root(self):
        """ find the local root of the cache tree, even if we start with skips """
        found_cache = None
        # try all the variants in case some symlinking weirdness is going on
        # TODO may want to detect and warn on that?
        root = self.__class__('/')
        for variant in set((self, self.absolute(), self.resolve())):
            for parent in chain((variant,), variant.parents):
                try:
                    if parent.cache:
                        found_cache = parent
                except (exc.NoCachedMetadataError, exc.NotInProjectError) as e:
                    # if we had a cache, went to the parent and lost it
                    # then we are at the root, assuming of course that
                    # there aren't sparse caches on the way up (down?) the tree
                    if found_cache is not None and found_cache != root:
                        return found_cache

            else:
                if found_cache and found_cache != root:
                    return found_cache

    @property
    def skip_cache(self):
        """ returns True if the path should not have a cache
            because the file has been ignored """
        if self.cache is None:
            parent_cache = self.parent.cache
            if parent_cache:
                rel_path = self.relative_to(parent_cache.anchor)
            else:
                root = self.find_cache_root()  # FIXME is it safe to cache this??
                rel_path = self.relative_to(root)
            return (rel_path.parts[0] in self._cache_class.cache_ignore or
                    # TODO more conditions
                    False)
        else:
            # FIXME technically not correct ...
            return True

    def dedupe(self, other, pretend=False):
        return self.cache.dedupe(other.cache, pretend=pretend)

    @property
    def id(self):  # FIXME reuse of the name here could be confusing, though it is technically correct
        """ THERE CAN BE ONLY ONE """
        # return self.checksum()  # doesn't quite work for folders ...
        # return self.as_posix()  # FIXME which one to use ...
        return self.sysid + ':' + self.as_posix()

    @property
    def created(self):
        self.meta.created

    def _stat(self):
        """ sometimes python just doesn't have what it takes """
        # we can't define this or use error trapping in self.stat() directly
        # because it will bolox other things that need stat to fail correctly
        cmd = ['stat', self.as_posix(), '-c', StatResult.stat_format]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, errs = p.communicate()
        # local results retain the quotes where pexpect does not
        out = out.strip(b'"').rstrip().rstrip(b'"')
        if not out and errs:
            p.returncode
            raise OSError

        return StatResult(out)

    @property
    def size(self):
        """ don't use this to populate meta, but meta computes a checksum
            so if you need anything less than the checksum don't get meta """
        try:
            st = self.stat()
        except OSError as e:
            st = self._stat()

        return st.st_size

    @property
    def meta(self):
        return self._meta_maker()

    @property
    def meta_no_checksum(self):
        return self._meta_maker(checksum=True)

    def _meta_maker(self, *, checksum=True, chunksize=None):
        """ setting chunksize will cause an etag to be calculated """
        if not self.exists():
            return PathMeta(
                id=self.sysid + ':' + self.as_posix(),
            )

        try:
            st = self.stat()
        except OSError as e:
            st = self._stat()

        # FIXME nanos vs millis ??
        change_tuple = (fs_metadata_changed_time,
                        fs_data_modified_time) = (st.st_ctime,
                                                  st.st_mtime)

        if hasattr(self, '_meta') and self._meta is not None:
            if self.__change_tuple == change_tuple:
                return self._meta

            old_meta = self._meta  # TODO log changes?


        self.__change_tuple = change_tuple  # TODO log or no?

        updated = datetime.fromtimestamp(fs_data_modified_time, tz=timezone.utc)
        # sanity check
        # td = (datetime.fromtimestamp(fs_data_modified_time, tz=timezone.utc)
              # - datetime(1970, 1, 1, tzinfo=timezone.utc))
        # assert int(fs_data_modified_time) == int(td.total_seconds())

        # these use our internal representation of timestamps
        # the choice of how to store them in xattrs, sqlite, json, etc is handled at those interfaces
        # replace with comma since it is conformant to the standard _and_
        # because it simplifies PathMeta as_path
        mode = oct(st.st_mode)
        self._meta = PathMeta(size=st.st_size,
                              created=None,
                              updated=updated,
                              checksum=self.checksum() if checksum else None,
                              etag=self.etag() if chunksize else None,
                              chunksize=chunksize,
                              id=self.id,
                              file_id=st.st_ino,  # pretend inode number is file_id ... oh wait ...
                              user_id=st.st_uid,
                              # keep in mind that a @meta.setter
                              # will require a coverter for non-unix uids :/
                              # man use auth is all bad :/
                              gid=st.st_gid,
                              mode=mode)

        return self._meta

    @meta.setter
    def meta(self, value):
        raise TypeError('Cannot set meta on LocalPath, it is a source of metadata.')

    def _data(self, ranges=tuple(), chunksize=None):
        """ request arbitrary subsets of data from an object """

        if chunksize is None:
            chunksize = self.chunksize

        with open(self, 'rb') as f:
            if not ranges:
                ranges = (0, None),
            else:
                # TODO validate ranges
                pass

            for start, end in ranges:
                f.seek(start, 2 if start < 0 else 0)
                if end is not None:
                    total = end - start
                    if total < chunksize:
                        nchunks = 0
                        last_chunksize = total
                    elif total > chunksize:
                        nchunks, last_chunksize = divmod(total, chunksize)

                    for _ in range(nchunks):  # FIXME boundscheck ...
                        yield f.read(chunksize)

                    yield f.read(last_chunksize)

                else:
                    while True:
                        data = f.read(chunksize)  # TODO hinting
                        if not data:
                            break

                        yield data

    @property
    def data(self):
        with open(self, 'rb') as f:
            while True:
                data = f.read(self.chunksize)  # TODO hinting
                if not data:
                    break

                yield data

    @data.setter
    def data(self, generator):
        if self.cache is not None:
            cmeta = self.cache.meta

        # FIXME do we touch a file, write the meta
        # and then write the data?
        # do we touch a temporary file, write the meta
        # unlink the symlink, and move the temp file in, and then write the data?
        # the order that we do this in is very important for robustness to failure
        # especially when updating a file ...
        # storing history in the symlink cache also an option?
        log.debug(f'writing to {self}')
        chunk1 = next(generator)  # if an error occurs don't open the file
        with open(self, 'wb') as f:
            f.write(chunk1)
            for chunk in generator:
                #log.debug(chunk)
                f.write(chunk)

        if self.cache is not None:  # FIXME cache
            if not self.cache.meta:
                self.cache.meta = cmeta  # glories of persisting xattrs :/
            # yep sometimes the xattrs get  blasted >_<
            assert self.cache.meta

    def _data_setter(self, generator):
        """ a data setter that can be used in a chain of generators """
        log.debug(f'writing to {self}')
        chunk1 = next(generator)  # if an error occurs don't open the file
        with open(self, 'wb') as f:
            f.write(chunk1)
            yield chunk1
            for chunk in generator:
                #log.debug(chunk)
                f.write(chunk)
                yield chunk

    def copy_to(self, target, force=False, copy_cache_meta=False):
        """ copy from a the current path object to a target path """
        if type(target) != type(self):
            target = self.__class__(target)

        if not target.exists() and not target.is_symlink() or force:
            target.data = self.data
        else:
            raise exc.PathExistsError(f'{target}')

        if copy_cache_meta:
            log.debug(f'copying cache meta {self.cache.meta}')
            target.cache_init(self.cache.meta)

    def copy_from(self, source, force=False, copy_cache_meta=False):
        """ copy from a source path to the current path object """
        if type(source) != type(self):
            source = self.__class__(source)

        source.copy_to(self, force=force, copy_cache_meta=copy_cache_meta)

    @property
    def children(self):
        if self.is_dir():
            if self.cache is not None and self == self.cache.anchor.local:
                cache_ignore = self._cache_class.cache_ignore
                # implemented this way we can still use Path to navigate
                # once we are inside local data dir, though all files there
                # are skip_cache -> True
                for path in self.iterdir():
                    if path.stem in cache_ignore:
                        continue

                    yield path
            else:
                yield from self.iterdir()

    @property
    def rchildren(self):
        if self.is_dir() and self.cache is not None and self == self.cache.anchor.local:
            for path in self.children:
                yield path
                yield from path.rchildren

        else:
            yield from self.rglob('*')

    def content_different(self):
        cmeta = self.cache.meta
        if cmeta.checksum:
            return self.meta.content_different(cmeta)
        else:
            # TODO use the index for this
            # but for now just pull down the remote file
            # NOTE: this is all handled behind the scenes
            # by cache.checksum now
            return self.checksum() != self.cache.checksum()

    def diff(self):
        """ This is a bit tricky because it means that we need to
            keep a shadow copy/cache of all the downloaded files in
            operations by default... """
        raise NotImplementedError

    def meta_to_remote(self):
        # pretty sure that we don't wan't this independent of data_to_remote
        # sort of cp vs cp -a and commit date vs author date
        raise NotImplementedError
        meta = self.meta
        # FIXME how do we invalidate cache?
        self.remote.meta = meta  # this can super duper fail

    def data_to_remote(self):
        raise NotImplementedError
        self.remote.data = self.data

    def annotations_to_remote(self):
        raise NotImplementedError
        self.remote.data = self.data

    def to_remote(self):  # push could work ...
        # FIXME in theory we could have an endpoint for each of these
        # The remote will handle that?
        # this can definitely fail
        raise NotImplementedError('need the think about how to do this without causing disasters')
        self.remote.meta = self.meta
        self.remote.data = self.data
        self.remote.annotations = self.annotations

    def etag(self, chunksize):
        """ chunksize is the etag cypher chunksize which is
            different than the data generator chunksize
            etag chunksize has be implemented so that it
            works correctly with any data generator chunksize """

        if self.is_file():
            m = etag(chunksize)
            for chunk in self.data:
                m.update(chunk)

            return m.digest()

        return


LocalPath._bind_flavours()


class RepoPath(RepoHelper, AugmentedPath): pass
RepoPath._bind_flavours()


class XopenPath(AugmentedPath):
    pass


class XopenWindowsPath(XopenPath, AugmentedPathWindows):
    _command = 'start'

    def xopen(self):
        """ open file using start """
        process = subprocess.Popen([self._command, self],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.STDOUT)


class XopenPosixPath(XopenPath, pathlib.PosixPath):
    _command = 'open' if sys.platform == 'darwin' else 'xdg-open'

    def xopen(self):
        """ open file using xdg-open """
        process = subprocess.Popen([self._command, self.as_posix()],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.STDOUT)

        return  # FIXME this doesn't seem to update anything beyond python??

        pid = process.pid
        proc = psutil.Process(pid)
        process_window = None
        while not process_window:  # FIXME ick
            sprocs = [proc] + [p for p in proc.children(recursive=True)]
            if len(sprocs) < 2:  # xdg-open needs to call at least one more thing
                sleep(.01)  # spin a bit more slowly
                continue

            wpids = [s.pid for s in sprocs][::-1]  # start deepest work up
            # FIXME expensive to create this every time ...
            disp = Display()
            root = disp.screen().root
            children = root.query_tree().children
            #names = [c.get_wm_name() for c in children if hasattr(c, 'get_wm_name')]
            try:
                by_pid = {c.get_full_property(disp.intern_atom('_NET_WM_PID'), 0):c for c in children}
            except Xlib.error.BadWindow:
                sleep(.01)  # spin a bit more slowly
                continue

            process_to_window = {p.value[0]:c for p, c in by_pid.items() if p}
            for wp in wpids:
                if wp in process_to_window:
                    process_window = process_to_window[wp]
                    break

            if process_window:
                name = process_window.get_wm_name()
                new_name = name + ' ' + self.resolve().as_posix()[-30:]
                break  # TODO search by pid is broken, but if you can find it it will work ...
                # https://github.com/jordansissel/xdotool/issues/14 some crazy bugs there
                command = ['xdotool', 'search','--pid', str(wp), 'set_window', '--name', f'"{new_name}"']
                subprocess.Popen(command,
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.STDOUT)
                print(' '.join(command))
                break
                process_window.set_wm_name(new_name)
                break
            else:
                sleep(.01)  # spin a bit more slowly


XopenPath._bind_flavours()

# any additional values
LocalPath._bind_sysid()
