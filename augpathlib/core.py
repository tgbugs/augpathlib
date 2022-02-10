import os
import sys
import errno
import shutil
import pathlib
import tempfile
import warnings
import mimetypes
import subprocess
from time import sleep
from errno import ELOOP, ENOENT, ENOTDIR, EBADF
from datetime import datetime, timezone
from functools import wraps
from itertools import chain
try:
    import magic  # from sys-apps/file consider python-magic ?
    _have_magic = True
except (AttributeError, ImportError, TypeError) as e:
    _have_magic = False

#import psutil  # import for experimental xopen functionality
#from Xlib.display import Display
#from Xlib import Xatom
import augpathlib as aug
from augpathlib import swap
from augpathlib import exceptions as exc
from augpathlib.meta import PathMeta
from augpathlib.utils import log, default_cypher, StatResult, etag
from augpathlib.utils import _bind_sysid_, AUG_XATTR_PREFIX

SPARSE_KEY = (AUG_XATTR_PREFIX + '.sparse')

_IGNORED_ERROS = (ENOENT, ENOTDIR, EBADF, ELOOP)
_IGNORED_WINERRORS = (
    123,  # 'The filename, directory name, or volume label syntax is incorrect' -> 22 EINVAL
)

if sys.platform == 'darwin':
    _IGNORED_ERROS += (errno.ENAMETOOLONG,)


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
    pathlib._IGNORED_WINERRORS += _IGNORED_WINERRORS
    _IGNORED_WINERRORS = pathlib._IGNORED_WINERRORS
    if sys.version_info < (3, 11):
        pathlib._IGNORED_ERROS += (ELOOP,)
        if sys.platform == 'darwin':
            # darwin gets very confused by self referential symlinks
            # and it seems that they stack up on eachother while being
            # dereferenced until the link name becomes too long
            pathlib._IGNORED_ERROS += (errno.ENAMETOOLONG,)
    else:
        pathlib._IGNORED_ERRNOS += (ELOOP,)
        if sys.platform == 'darwin':
            pathlib._IGNORED_ERRNOS += (errno.ENAMETOOLONG,)
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


class EatHelper:
    """ Extended attributes helper """

    @staticmethod
    def _base_helpers(pos_helpers, win_helpers):
        pos_helpers = tuple(set(pos_helpers + (XattrHelper,)))
        win_helpers = tuple(set(win_helpers + (ADSHelper,)))
        return pos_helpers, win_helpers

    @classmethod
    def _bind_flavours(cls, pos_helpers=tuple(), win_helpers=tuple()):
        super()._bind_flavours(*EatHelper._base_helpers(pos_helpers, win_helpers))


class ADSHelper(EatHelper):
    """ Windows NTFS equivalent of Xattrs is Alternate Data Streams
        This class allows ADS to pretend to work like xattrs.
    """

    _sparse_key = SPARSE_KEY

    @staticmethod
    def _key_convention(key, namespace):
        return namespace + '.' + key  # FIXME maybe include xattrs. as well ??

    def _stream(self, name):
        # FIXME single char folder names are completely evil here
        # you they are not absolute and they resolve to themselves
        *start, last = self.parts
        if not start and len(last) == 1:
            # single letter file names with no extension
            # masquerade as drive letters on windows and
            # there seems to be nothing we can do about it
            raise ValueError('windows and single letter file names dont get along')

        return AugmentedPath(*start, './' + last + ':' + name)

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

    def delxattr(self, key, fail=False, namespace=XATTR_DEFAULT_NS):
        name = self._key_convention(key, namespace)
        stream = self._stream(name)
        try:
            stream.unlink()
        except FileNotFoundError as e:
            if fail:
                raise exc.NoStreamError((self, key)) from e

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
        # FIXME this is almost certainly bugged for Path('.') too
        for k, v in xattr_dict.items():
            self.setxattr(k, v, namespace=namespace)

    def getxattr(self, key, namespace=XATTR_DEFAULT_NS):
        # we don't deal with types here, we just act as a dumb store
        name = self._key_convention(key, namespace)
        try:
            with open(self._stream(name), 'rb') as f:
                return f.read()

        except FileNotFoundError as e:
            raise exc.NoStreamError((self, key)) from e

    def _xattrs(self):
        if not self.is_absolute():
            # the nature of the '.' path on windows means that
            # _streams will severly misbehave so we resolve the path
            # before retrieving streams, note that you can shoot
            # yourself in the foot if you are carrying around an
            # unresolved path and are not where you think you are
            self = self.resolve()

        out = {}
        for stream in self._streams:
            _base, k = stream.name.split(':', 1)
            with open(stream, 'rb') as f:
                v = f.read()

            out[k] = v

        return out

    def xattrs(self, namespace=XATTR_DEFAULT_NS):
        # FIXME broken for Path('.') causes an error in _stream
        # FIXME broken if run with path: path.xattrs() -> returns {}
        # I think that on windows these paths must always be resolved?
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

    _sparse_key = SPARSE_KEY.encode()

    def delxattr(self, key, fail=False, namespace=XATTR_DEFAULT_NS):
        try:
            xattr.remove(self.as_posix(), key, namespace=namespace)
        except OSError as e:
            if fail or e.errno != 61:  # 61 -> No data available
                raise e

    def setxattr(self, key, value, namespace=XATTR_DEFAULT_NS):
        if not isinstance(value, bytes):  # checksums
            raise TypeError('setxattr only accepts values already '
                            f'encoded to bytes!\n{value!r}')
        else:
            bytes_value = value

        xattr.set(self.as_posix(), key, bytes_value, namespace=namespace)

    def setxattrs(self, xattr_dict, namespace=XATTR_DEFAULT_NS):
        for k, v in xattr_dict.items():
            self.setxattr(k, v, namespace=namespace)

    def getxattr(self, key, namespace=XATTR_DEFAULT_NS):
        # we don't deal with types here, we just act as a dumb store
        try:
            return xattr.get(self.as_posix(), key, namespace=namespace)
        except OSError as e:
            if e.errno == errno.ENODATA or e.errno == errno.ENOATTR:
                raise exc.NoStreamError((self, key)) from e
            else:
                raise e

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

    if sys.version_info >= (3, 10):
        def __new__(cls, *args, **kwargs):
            if cls is cls.__abstractpath:
                cls = cls.__windowspath if os.name == 'nt' else cls.__posixpath
            self = cls._from_parts(args)
            if not self._flavour.is_supported:
                raise NotImplementedError("cannot instantiate %r on your system"
                                        % (cls.__name__,))
            return self

    else:
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

    def to_relative(self, other):
        """ unfortunately pathlib's relative_to should
            actually be called relative_from because it is the
            relative path FROM the other path to the current path
            or rather it is relative_to_here_from_there which is
            not good because the final from is truncated, confusing
            everything also 'from here to there' is much more natural """

        rp = self.relative_to(other)
        return self.__class__(*['..' for _ in rp.parts[:-1]])

    def relative_path_to(self, other):  # from here
        """ an actual implementation of relative_to that works ... """
        return other.relative_path_from(self)

    def relative_path_from(self, other):  # to here
        """ relative path to self from the other path """
        base = self.commonpath(other)
        ort = other.to_relative(base)
        return ort / self.relative_to(base)

    def rename(self, target):
        os.rename(self, target)

    def swap_carefree(self, target):
        """ swap two paths, use atomic if available on the system
            and if the paths are on the same device otherwise fail

            side note: there doesn't seem to be a standard name for
            the superset of atomic and non-atomic, this operations
            is definitely not non-atomic, it is maybe-atomic, but
            maybe-atomic is too optimistic, thus carefree seems
            appropriately ... disinterested in the exact semantics """
        try:
            self.swap(target)
        except (Exception, NotImplementedError) as e:  # TODO clearer error handling
            temp_str = tempfile.mkdtemp(dir=target.parent)
            temp_dir = pathlib.Path(temp_str)
            temp = temp_dir / target.name
            # rename target -> temp
            # rename self -> target
            # rename target -> self
            target.rename(temp)
            self.rename(target)
            temp.rename(self)
            temp_dir.rmdir()

    if sys.platform != 'linux':  # just look at us not using pathlib's infra ...
        _swap = swap.swap_not_implemented
    else:
        _swap = swap.swap_linux

    def swap(self, target):
        """ atomic swap of two paths

            this will raise and error in the following cases
            1. if the system has no atomic swap function such as renameat2
            2. if the paths to be swapped are on different devices
            3. if either path does not exist
        """
        se = self.exists()
        te = target.exists()
        if not se:
            msg = f'Both self and target must exist self does not! {self}'
            raise FileNotFoundError(msg)
        elif not te:
            msg = f'Both self and target must exist target does not! {target}'
            raise FileNotFoundError(msg)

        sd = self.stat().st_dev
        td = target.stat().st_dev
        if sd != td:
            msg = f'Self and target must be on the same device! {sd} != {td}'
            raise ValueError(msg) # FIXME find or make the correct error type

        self._swap(target)

    def rmtree(self, ignore_errors=False, onerror=None, DANGERZONE=False):
        """ DANGER ZONE """
        # FIXME make this atomic by renaming to a random name
        # that doesn't exist and then calling rmtree on that
        if not self.is_absolute():
            raise exc.WillNotRemovePathError(f'Only absolute paths can be removed recursively. {self}')

        if not (DANGERZONE is True):  # prevent python type coersion
            # TODO test in a chroot
            lenparts = len(self.parts)
            if lenparts <= 2:
                raise exc.WillNotRemovePathError(f'Will not remove top level paths. {self}')
            elif lenparts <= 3 and 'home' in self.parts:
                raise exc.WillNotRemovePathError(f'Will not remove home directories. {self}')
            elif self == pathlib.Path.cwd():
                raise exc.WillNotRemovePathError(f'Will not remove current working directory. {self}')

        try:
            if self.is_dir():
                if self.is_symlink():
                    # see the note in shutil.rmtree about race conditions
                    # for now we are going to hard fail if this case occures
                    raise OSError("Cannot call rmtree on a symbolic link")

                for path in self.iterdir():
                    if path.is_symlink():
                        # mimic shutil behavior and don't accidentally
                        # recurse through symlinks (keyword being curse)
                        path.unlink()
                    else:
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
        if self.is_dir():
            return 'inode/directory'  # matches _magic_mimetype

        if not self.is_absolute():  # needed for safe as_uri
            if self.is_broken_symlink():
                self = self.absolute()
            else:
                self = self.resolve()

        mime, encoding = mimetypes.guess_type(self.as_uri())
        if mime:
            return mime
        elif hasattr(self, '_suffix_mimetypes') and self._suffix_mimetypes:
            # FIXME TODO make a real interface for these
            suffixes = tuple(self.suffixes)
            if suffixes in self._suffix_mimetypes:
                return self._suffix_mimetypes[suffixes]

    @property
    def encoding(self):
        if not self.is_absolute():  # needed for safe as_uri
            self = self.resolve()

        mime, encoding = mimetypes.guess_type(self.as_uri())
        if encoding:
            return encoding

    @property
    def _magic_mimetype(self):
        """ This can be slow because it has to open the files. """
        if self.exists():
            try:
                if hasattr(magic, 'detect_from_filename'):
                    # sys-apps/file python-magic api
                    return magic.detect_from_filename(self).mime_type
                else:
                    # python-magic
                    return magic.from_file(self.as_posix(), mime=True)
            except NameError as e:
                if not _have_magic:
                    msg = ('no module magic found from either python-magic '
                           'or from libmagic python bindings')
                    raise ModuleNotFoundError(msg) from e
                else:
                    raise e

    def checksum(self, cypher=default_cypher, extra_cyphers=tuple()):
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
            extra = [c() for c in extra_cyphers]
            for chunk in self.data:
                m.update(chunk)
                for me in extra:
                    me.update(chunk)

            if extra_cyphers:
                return tuple(_.digest() for _ in (m, *extra))
            else:
                return m.digest()

    def copy_to(self, target, force=False):
        """ copy from a the current path object to a target path """
        if not target.exists() and not target.is_symlink() or force:
            # FIXME copytree does not happen, so sources cannot currently
            # be directories, what are the downsides of homogenizing that?
            shutil.copy2(self, target)

        else:
            raise exc.PathExistsError(f'{target}')

    def copy_from(self, source, force=False, copy_cache_meta=False):
        """ copy from a source path to the current path object """
        source.copy_to(self, force=force)

    def copy_outto(self, target, force=False):
        """ copy the current path out to a target directory """
        if not target.is_dir():
            raise NotADirectoryError(f'{target} is not a directory')

        target_file = target / self.name
        self.copy_to(target_file, force=force)
        return target_file

    def copy_infrom(self, source, force=False):
        """ copy into the current directory a file from somewhere else """
        return source.copy_outto(self, force=force)


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


class EatPath(EatHelper, AugmentedPath):

    # NOTE _sparse_key is set on each helper

    def _sparse_root(self):
        parent = self.parent
        if self != parent:
            try:
                return self.getxattr(self._sparse_key) == b'1'
            except exc.NoStreamError:
                return parent._sparse_root()

    def is_sparse(self):
        return self._sparse_root() is not None

    def _clear_sparse(self):
        self.delxattr(self._sparse_key)

    def _mark_sparse(self):
        self.setxattr(self._sparse_key, b'1')


EatPath._bind_flavours()


class LocalPath(EatPath, AugmentedPath):
    # local data about remote objects

    chunksize = 4096  # make the data generator chunksize visible externally
    _cache_class = None  # must be defined by child classes
    sysid = None  # set below

    _bind_sysid = classmethod(_bind_sysid_)

    def __new__(cls, *args, **kwargs):
        self = super().__new__(cls, *args, **kwargs)
        #if args and isinstance(args[0], LocalPath):
            # XXX I'm sure this will break cases where we want to change from
            # one cache class to another by changing which local path class we
            # construct, but wow this is way too far gone already
            # XXX AAAAND it has, and it is extremely insanity inducing and
            # hard to debug
            #self._cache_class = args[0]._cache_class

        return self

    @classmethod
    def setup(cls, cache_class, remote_class_factory):
        """ call this once to bind everything together """

        cn = self.__class__.__name__
        warnings.warn(f'{cn}.setup is deprecated please switch to RemotePath._new',
                      DeprecationWarning,
                      stacklevel=2)

        cache_class.setup(cls, remote_class_factory)

    #def __truediv__(self, other):
        # TODO need a way to get the remote relative to the anchor for this
        #if isinstance(other, aug.RemotePath):
            #super().__truediv__(other.as_path())
        #else:
            #super().__truediv__(other)

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
                self._cache = self._cache_class(self)
                # +we don't have to assign here because cache does it+ XXX
                # assign here anyway because it makes it clear what is going on
                # and avoids spooky action at a distance and avoids implicit
                # expectations on the cache constructor
            except exc.NoCachedMetadataError as e:
                #log.error(e)
                return None
            except TypeError as e:
                if self._cache_class is None:
                    return None
                else:
                    raise e

        return self._cache

    @property
    def cache_id(self):
        """ abstraction violating but fast way to get cache.id """
        raise NotImplementedError('implement in subclass')

    def cache_init(self, id_or_meta, anchor=False):
        """ wow it took way too long to realize this was the way to do it >_<
            **kwargs are passed to _cache_class and _remote_class.init """

        if self.cache and self.cache.meta:
            raise exc.CacheExistsError(f'{self.cache}\n'
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

    def mkdir_cache(self, remote):  # XXX hack around my idiocy / desire to not hit the network
        """ wow side effects everywhere
            given a remote, create the local folder structure
            and attach the remote metadata as cache

            I think this is implemented on LocalPath because only the
            local parent exists and the cache can only come into being after
            a folder is created since there is no point in making a symlink
            just to replace it with a folder, that just thrashes disk """

        cc = self._cache_class
        rc = cc._remote_class
        for parent in reversed(tuple(remote.parents)):
            # remote as_path is always a PurePosixPath relative to the
            # anchor and does not include the anchor
            local_path = cc.anchor.local / parent.as_path()
            if not local_path.exists():
                local_path.mkdir()
                rc(parent, cache=cc(local_path, remote=parent, meta=parent.meta))

    def mkdir_remote(self, parents=False):  # XXX hack around my idiocy / desire to not hit the network
        # FIXME this should really make a cache with a temporary id ...
        # that way we have a staging area ... but that is a bit too much for right now
        if self.cache and self.remote and self.remote.exists():
            # it is ok to check all of these to make sure that
            # things don't go stale, under most circumstances
            # self.cache will be None when this is called
            raise exc.RemotePathExistsError(self.remote)

        if self.parent.cache:
            remote = self.parent.remote._mkdir_child(self.name)
            # FIXME see the note about my dumbness wrt paths
            # the whole remote appraoch nees a complete rework so that the remotes
            # just act like paths rathern than forcing them to exist which causes all
            # sorts of awkwardness, including the RemoteMaybeExists issue
            self.mkdir(exist_ok=True)
            if remote.cache is None:
                self.cache_init(remote.meta)
                remote._cache = self.cache

            return remote

        elif parents:
            for parent in self.parents:
                c = parent.cache
                if c:
                    parts = self.relative_path_from(parent).parts
                    for p in parts:
                        print(self, parent, parts, p)
                        remote = parent.cache.remote._mkdir_child(p)
                        local = parent / p
                        local.mkdir(exist_ok=True)
                        local.cache_init(remote.meta)
                        parent = local
                        #parent = remote.cache.local  # fails due to cache is None
                        # FIXME will this work as expected ?! and just put the metadata on the
                        # existing folder or will it barf? the answer is no, cache is missing

                    remote._cache = local.cache
                    return remote
        else:
            raise FileNotFoundError('missing parent for {self} and parents=False')

    def find_cache_root(self, fail=False):
        """ find the local root of the cache tree, even if we start with skips """
        if self.is_broken_symlink():
            return self.parent.find_cache_root(fail=fail)

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
                    if fail:
                        raise e

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
                # FIXME is it safe to cache the results of finding the root??
                root = self.find_cache_root(fail=True)
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
        return self._meta_maker(checksum=False)

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

    def _write_chunks_ntfs(self, generator):
        # SO. It turns out that open(thing, 'wb') has fundamentally different
        # semantics on posix and windows (wheeeeeeeeeee!) on posix it keeps
        # xattrs intact, on windows it erases them AAAAAAAAAAAAAAAAAAAAAAA
        chunk1 = next(generator)  # if an error occurs don't open the file FIXME I think this might be causing the zero size files?
        with open(self, 'ab') as f:
            f.seek(0)
            f.truncate()
            f.write(chunk1)
            for chunk in generator:
                #log.debug(chunk)
                f.write(chunk)

    def _write_chunks_posix(self, generator):
        chunk1 = next(generator)  # if an error occurs don't open the file FIXME I think this might be causing the zero size files?
        with open(self, 'wb') as f:
            f.write(chunk1)
            for chunk in generator:
                #log.debug(chunk)
                f.write(chunk)

    _write_chunks = (
        _write_chunks_ntfs if os.name == 'nt' else _write_chunks_posix)

    @data.setter
    def data(self, generator):
        cache = self.cache
        if cache is not None:
            cmeta = cache.meta
        else:
            assert self.cache is None

        # FIXME do we touch a file, write the meta
        # and then write the data?
        # do we touch a temporary file, write the meta
        # unlink the symlink, and move the temp file in, and then write the data?
        # the order that we do this in is very important for robustness to failure
        # especially when updating a file ...
        # storing history in the symlink cache also an option?
        log.debug(f'writing to {self}')
        self._write_chunks(generator)
        if cache is not None:  # FIXME cache
            if not cache.meta:
                # XXX FIXME when this fails to set things downstream fail as well
                # so we see things like None type has no attribute xyz because the
                # cached metadata didn't get set correctly, this is happening in
                # some subclass where cache.meta doesn't have a setter
                cache.meta = cmeta  # glories of persisting xattrs :/
            # yep sometimes the xattrs get  blasted >_<
            assert cache.meta
            assert self.cache.meta

    def _data_setter(self, generator, append=False):  # FIXME ntfs ads issues
        """ a data setter that can be used in a chain of generators """
        # FIXME if the generator can silently fail that is very very bad news ...
        # but how/why would they be silently failing ??!
        log.debug(f'writing to {self}')
        chunk1 = next(generator)  # if an error occurs don't open the file
        with open(self, 'ab' if append else 'wb') as f:
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
            if (self.cache is not None and
                # relative paths inside may have a cache but no anchor
                # the anchor itself can be relative and have an anchor
                # so we test to see if there is an anchor first
                self.cache.anchor and
                self == self.cache.anchor.local):
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
        if (self.is_dir() and self.cache is not None and
            # relative paths inside may have a cache but no anchor
            # the anchor itself can be relative and have an anchor
            # so we test to see if there is an anchor first
            self.cache.anchor and
            self == self.cache.anchor.local):
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


class XopenHelper:
    @staticmethod
    def _base_helpers(pos_helpers, win_helpers):
        pos_helpers = tuple(set(pos_helpers + (XopenPosixHelper,)))
        win_helpers = tuple(set(win_helpers + (XopenWindowsHelper,)))
        return pos_helpers, win_helpers

    @classmethod
    def _bind_flavours(cls, pos_helpers=tuple(), win_helpers=tuple()):
        super()._bind_flavours(*XopenHelper._base_helpers(pos_helpers, win_helpers))


class XopenWindowsHelper(XopenHelper):
    _command = 'start'

    def xopen(self, command=None):
        """ open file using start or `command' if provided """
        if command is None or not isinstance(command, str):
            command = self._command

        process = subprocess.Popen([command, self],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.STDOUT)


class XopenPosixHelper(XopenHelper):
    _command = 'open' if sys.platform == 'darwin' else 'xdg-open'

    def xopen(self, command=None):
        """ open file using xdg-open or `command' if provided """
        if command is None or not isinstance(command, str):
            command = self._command

        process = subprocess.Popen([command, self.as_posix()],
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


class XopenPath(XopenHelper, AugmentedPath):
    pass


XopenPath._bind_flavours()

# any additional values
LocalPath._bind_sysid()
