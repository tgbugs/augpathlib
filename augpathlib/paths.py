import os
import sys
import pathlib
import mimetypes
import subprocess
from time import sleep
from errno import ELOOP, ENOENT, ENOTDIR, EBADF
from pathlib import PosixPath, PurePosixPath
from datetime import datetime, timezone
from functools import wraps
from itertools import chain
try:
    import magic  # from sys-apps/file consider python-magic ?
except ImportError:
    pass

import xattr
import psutil
from git import Repo
from dateutil import parser
from Xlib.display import Display
from Xlib import Xatom
from augpathlib import exceptions as exc
from augpathlib.utils import log, default_cypher, StatResult, etag
from augpathlib.utils import _bind_sysid_, LOCAL_DATA_DIR
from augpathlib.pathmeta import PathMeta

_IGNORED_ERROS = (ENOENT, ENOTDIR, EBADF, ELOOP)


def _ignore_error(exception):
    return (getattr(exception, 'errno', None) in _IGNORED_ERROS)


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


# remote data about remote objects -> remote_meta
# local data about remote objects -> cache_meta
# local data about local objects -> meta
# remote data about local objects <- not relevant yet? or is this actually rr?

# TODO by convetion we could store 'children' of files in a .filename.ext folder ...
# obviously this doesn't always work
# and we would only want to do this for files that actually had annotations that
# needed a place to live ...

class RemotePath:
    """ Remote data about a remote object. """

    _cache_class = None
    _debug = False

    # we use a PurePath becuase we still want to key off this being local path
    # but we don't want any of the local file system operations to work by accident
    # so for example self.stat should return the remote value not the local value
    # which is what would happen if we used a PosixPath as the base class

    # need a way to pass the session information in independent of the actual path
    # abstractly having remote.data(global_id_for_local, self)
    # should be more than enough, the path object shouldn't need
    # to know that it has a remote id, the remote manager should
    # know that

    @classmethod
    def _new(cls, local_class, cache_class):
        # FIXME 1:1ness issue from local -> cache
        # probably best to force the type of the cache
        # to switch if there are multiple remote mappings
        # since there can be only 1 local file with the same
        # path, a composite cache or a multi-remote cache
        # seems a bit saner, or require explicit switching of
        # the active remote if one-at-a-time semantics are desired
        # see also the note RemoteFactory.__new__

        newcls = type(cls.__name__,
                      (cls,),
                      dict(_local_class=local_class,
                           _cache_class=cache_class))

        local_class._remote_class = newcls
        local_class._cache_class = cache_class
        cache_class._remote_class = newcls
        cache_class._local_class = local_class

        return newcls

    @classmethod
    def init(cls, identifier):
        """ initialize the api from an identifier and bind the root """
        if not hasattr(cls, '_api'):
            cls._api = cls._api_class(identifier)
            cls.root = cls._api.root

        else:
            raise ValueError(f'{cls} already bound an api to {cls._api}')

    @classmethod
    def dropAnchor(cls, parent_path=None):
        """ When ya know where ya want ta land ... """
        if not hasattr(cls, '_cache_anchor'):
            if parent_path is None:
                parent_path = cls._local_class.cwd()
            else:
                parent_path = cls._local_class(parent_path)

            root = cls(cls.root)  # FIXME formalize the use of root
            path = parent_path / root.name
            if not path.exists():
                if root.is_file():
                    raise NotImplementedError('Have not implemented mapping for individual files yet.')

                elif root.is_dir():
                    path.mkdir()

                else:
                    raise NotImplementedError(f'What\'s a {root}?!')

            elif list(path.children):
                raise exc.NotEmptyError(f'has children {path}')

            cls._cache_anchor = path.cache_init(root.id, anchor=True)
            return cls._cache_anchor

        else:
            raise ValueError(f'already anchored to {cls._cache_anchor}')

    @classmethod
    def setup(cls, local_class, cache_class):
        """ call this once to bind everything together """
        cache_class.setup(local_class, cls)

    def bootstrap(self, recursive=False, only=tuple(), skip=tuple()):
        #self.cache.remote = self  # duh
        # if you forget to tell the cache you exist of course it will go to
        # the internet to look for you, it isn't quite smart enough and
        # we're trying not to throw dicts around willy nilly here ...
        return self.cache.bootstrap(self.meta, recursive=recursive, only=only, skip=skip)

    def __init__(self, thing_with_id, cache=None):
        if isinstance(thing_with_id, str):
            id = thing_with_id
        elif isinstance(thing_with_id, PathMeta):
            id = thing_with_id.id
        elif isinstance(thing_with_id, RemotePath):
            id = thing_with_id.id
        else:
            raise TypeError(f'Don\'t know how to initialize a remote from {thing_with_id}')

        self._id = id
        if cache is not None:
            self._cache = cache
            self.cache._remote = self

        self._errors = []

    @property
    def id(self):
        return self._id

    @property
    def errors(self):
        raise NotImplementedError

    @property
    def cache(self):
        if hasattr(self, '_cache_anchor') and self._cache_anchor is not None:
            return self._cache
        else:
            # cache is not real
            class NullCache:
                @property
                def _are_we_there_yet(self, remote=self):
                    # this is useless since these classes are ephemoral
                    if hasattr(remote, '_cache_anchor') and remote._cache_anchor is not None:
                        remote.cache_init()

                def __rtruediv__(self, other):
                    return None

                def __truediv__(self, other):
                    return None

            return NullCache()

    def cache_init(self):
        return self._cache_anchor / self

    @property
    def _cache(self):
        """ To catch a bad call to set ... """
        if hasattr(self, '_c_cache'):
            return self._c_cache

    @_cache.setter
    def _cache(self, cache):
        if not isinstance(cache, CachePath):
            raise TypeError(f'cache is a {type(cache)} not a CachePath!')

        self._c_cache = cache

    def _cache_setter(self, cache, update_meta=True):
        cache._remote = self
        # FIXME in principle
        # setting cache needs to come before update_meta
        # in the event that self.meta is missing file_id
        # if meta updater fails we unset self._c_cache
        self._cache = cache
        if update_meta:
            try:
                cache._meta_updater(self.meta)
            except BaseException as e:
                self._c_cache = None
                delattr(self, '_c_cache')
                raise e

    @property
    def local(self):
        return self.cache.local  # FIXME there are use cases for bypassing the cache ...

    @property
    def local_direct(self):
        # kind of uninstrumeted ???
        return self._local_class(self.as_path())

    @property
    def anchor(self):
        """ the semantics of anchor for remote paths are a bit different
            RemotePath code expects this function to return a RemotePath
            NOT a string as is the case for core pathlib. """
        raise NotImplementedError

    @property
    def _meta(self):  # catch stragglers
        raise NotImplementedError

    def refresh(self):
        """ Refresh the local in memory metadata for this remote.
            Implement actual functionality in your subclass. """

        raise NotImplementedError
        # could be fetch or pull, but there are really multiple pulls as we know

        # clear the cached value for _meta
        if hasattr(self, '_meta'):
            delattr(self, '_meta')

    @property
    def data(self):
        raise NotImplementedError
        self.cache.id
        for chunk in chunks:
            yield chunk

    @property
    def meta(self):
        # on blackfynn this is the package id or object id
        # this will error if there is no implementaiton if self.id
        raise NotImplementedError
        #return PathMeta(id=self.id)

    def _meta_setter(self, value):
        raise NotImplementedError

    @property
    def annotations(self):
        # these are models etc in blackfynn
        yield from []
        raise NotImplementedError

    def as_path(self):
        """ returns the relative path construction for the child so that local can make use of it """
        return PurePosixPath(*self.parts)

    def _parts_relative_to(self, remote, cache_parent=None):
        parent_names = []  # FIXME massive inefficient due to retreading subpaths :/
        # have a look at how pathlib implements parents
        parent = self.parent
        if parent != remote:
            parent_names.append(parent.name)
            # FIXME can this go stale? if so how?
            #log.debug(cache_parent)
            if cache_parent is not None and parent.id == cache_parent.id:
                    for c_parent in cache_parent.parents:
                        if c_parent is None:
                            continue
                        elif c_parent.name == remote.name:  # FIXME trick to avoid calling id
                            parent_names.append(c_parent.name)  # since be compare one earlier we add here
                            break
                        else:
                            parent_names.append(c_parent.name)

            else:
                for parent in parent.parents:
                    if parent == remote:
                        break
                    elif parent is None:
                        continue  # value error incoming
                    else:
                        parent_names.append(parent.name)

                else:
                    self._errors += ['file-deleted']
                    msg = f'{remote} is not one of {self}\'s parents'
                    log.error(msg)
                    #raise ValueError()

        args = (*reversed(parent_names), self.name)
        return args

    @property
    def parts(self):
        if self == self.anchor:
            return tuple()

        if not hasattr(self, '_parts'):
            if self.cache:
                cache_parent = self.cache.parent
            else:
                cache_parent = None

            self._parts = tuple(self._parts_relative_to(self.anchor, cache_parent))

        return self._parts

    @property
    def parent(self):
        """ The atomic parent operation as understood by the remote. """
        raise NotImplementedError

    @property
    def parents(self):
        parent = self.parent
        while parent:
            yield parent
            parent = parent.parent

    @property
    def children(self):
        # uniform interface for retrieving remote hierarchies decoupled from meta
        raise NotImplementedError

    @property
    def rchildren(self):
        # uniform interface for retrieving remote hierarchies decoupled from meta
        yield from self._rchildren()

    def _rchildren(self, create_cache=True):
        raise NotImplementedError

    def children_pull(self, existing):
        # uniform interface for asking the remote to
        # update children using its own implementation
        raise NotImplementedError

    def iterdir(self):
        # I'm guessing most remotes don't support this
        raise NotImplementedError

    def glob(self, pattern):
        raise NotImplementedError

    def rglob(self, pattern):
        raise NotImplementedError

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return f'{self.__class__.__name__}({self.id!r})'


class AugmentedPath(PosixPath):
    """ extra conveniences, mostly things that are fixed in 3.7 using IGNORE_ERROS """

    _stack = []  # pushd and popd
    count = 0
    _debug = False  # sigh

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

    def readlink(self):
        """ this returns the string of the link only due to cycle issues """
        link = os.readlink(self)
        if isinstance(link, bytes):  # on pypy3 readlink still returns bytes
            link = link.decode()

        #log.debug(link)
        return PurePosixPath(link)

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

        return os.access(self, mode, follow_symlinks=follow_symlinks)

    def commonpath(self, other):
        return self.__class__(os.path.commonpath((self, other)))

    def rename(self, target):
        os.rename(self, target)

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
            print(*reversed(AugmentedPath._stack), self.cwd())
            return path
        else:
            log.warning('popd: directory stack empty')

    def __enter__(self):
        if self.is_dir():
            self._entered_from = self.cwd()
            self.chdir()
            return self
        else:
            super().__enter__(self)

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
            return magic.detect_from_filename(self).mime_type

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


class CachePath(AugmentedPath):
    # CachePaths this needs to be a real path so that it can navigate the local path sturcture
    """ Local data about remote objects.
        This is where the mapping between the local id (aka path)
        and the remote id lives. In a git-like world this is the
        cache/index/whatever we call it these days 

        This is the bridge class that holds the mappings.
        Always start bootstrapping from one of these classes
        since it has both the local and remote identifiers,
        and therefore can be called and used before specifying
        the exact implementations for the local and remote objects.
    """

    _local_data_dir = LOCAL_DATA_DIR
    cache_ignore = _local_data_dir, '.git',  # TODO

    _local_class = None
    _remote_class_factory = None

    _backup_cache = None
    _not_exists_cache = None

    def __enter__(self):
        if self.is_dir():
            self._entered_from = self.local.cwd()  # caches can't exist outside their anchor anymore
            self.chdir()
            return self
        else:
            super().__enter__(self)

    @classmethod
    def setup(cls, local_class, remote_class_factory):
        """ call this once to bind everything together """
        cls._local_class = local_class
        cls._remote_class_factory = remote_class_factory
        local_class._cache_class = cls
        remote_class_factory._cache_class = cls

    @property
    def local_class(self):
        if self.is_helper_cache:
            return self._cache_parent.local_class

        return self._local_class

    def __new__(cls, *args, meta=None, remote=None, **kwargs):
        # TODO do we need a version of this for the locals
        # and remotes? I don't think we create 'alternate' remotes or locals ...

        self = super().__new__(cls, *args, **kwargs)

        # clone any existing locals and remotes
        if args:
            path = args[0]
            if isinstance(path, CachePath):
                self._cache_parent = path
                if hasattr(self._cache_parent, '_in_bootstrap'):
                    # it is ok to do this and not clean up because
                    # child caches are ephemoral
                    self._in_bootstrap = self._cache_parent._in_bootstrap

                if path.local is not None:  # this might be the very fist time local is called
                    self._local = path.local

            elif isinstance(path, LocalPath):
                self._local = path
                path._cache = self

            elif isinstance(path, RemotePath):
                #self._remote = path
                #self.meta = path.meta
                # in order for this to work the remote has to already
                # know where the cache should live, which it doesn't
                # use move instead for cases where the semantics are well defined
                raise TypeError('Not entirely sure what to do in this case ...')

        return self

    def __init__(self, *args, meta=None, remote=None, **kwargs):
        if remote:
            self._remote = remote
            self._meta_setter(remote.meta)
        elif meta:
            self._meta_updater(meta)
        else:
            if self.meta is None:
                raise exc.NoCachedMetadataError(self.local)

        super().__init__()

    @property
    def anchor(self):
        raise NotImplementedError('You need to define the rule for determining '
                                  'the local cache root for \'remote\' paths. '
                                  'These are sort of like pseudo mount points.')

    @property
    def trash(self):
        # FIXME mkdir and put it in a more conventional location
        return self.anchor.local.parent / '.trash'

    def crumple(self):  # FIXME change name to something more obvious ...
        trashed = self.trash / f'{self.parent.id}-{self.id}-{self.name}'
        try:
            self.rename(trashed)
        except OSError as e:
            if e.errno == 36:  # File name too long  # SIGH
                trashed = self.trash / self.name  # FIXME SIGH
                log.critical(f'Long name {self.name}')
                self.rename(trashed)
            else:
                raise e

        return trashed

    @property
    def local_data_dir(self):
        return self.anchor.local / self._local_data_dir

    @property
    def local_objects_dir(self):
        """ sort of like .git/objects """
        return self.local_data_dir / 'objects'

    @property
    def local_object_cache_path(self):
        # FIXME probably need the 2 char directory convention
        # to limit directory size
        return self.local_objects_dir / self.cache_key

    @property
    def is_helper_cache(self):
        return hasattr(self, '_cache_parent')

    def __truediv__(self, key):
        # basically RemotePaths are like relative CachePaths ... HRM
        # they are just a name and an id ... the id of their parent
        # root needs to match the id of the cache ... which it usually
        # does by construction
        if isinstance(key, RemotePath):
            # FIXME not just names but relative paths???
            remote = key
            try:
                child = self._make_child(remote._parts_relative_to(self.remote, self.parent), remote)
            except AttributeError as e:
                raise exc.SparCurError('aaaaaaaaaaaaaaaaaaaaaa') from e

            return child
        else:
            raise TypeError('Cannot construct a new CacheClass from an object '
                            f'without an id and a name! {key}')

    def __rtruediv__(self, cache):
        """ key is a subclass of self.__class__ """
        # I assume that this happens when a cache is constructed from
        # an relative cache?
        out = self._from_parts([cache.name] + self._parts, init=False)
        out._init()
        cache.remote._cache_setter(out)  # this seems more correct?
        #out._meta_setter(cache.meta)
        return out

    def _make_child(self, args, remote):
        drv, root, parts = self._parse_args(args)
        drv, root, parts = self._flavour.join_parsed_parts(
            self._drv, self._root, self._parts, drv, root, parts)
        child = self._from_parsed_parts(drv, root, parts, init=False)  # short circuits
        child._init()
        if isinstance(remote, RemotePath):
            remote._cache_setter(child)
        else:
            raise ValueError('should not happen')

        return child

    def bootstrap(self, meta, *,
                  parents=False,
                  recursive=False,
                  fetch_data=False,
                  size_limit_mb=2,
                  only=tuple(),
                  skip=tuple()):
        try:
            self._in_bootstrap = True
            return  list(self._bootstrap(meta,
                                         parents=parents,
                                         recursive=recursive,
                                         fetch_data=fetch_data,
                                         size_limit_mb=size_limit_mb,
                                         only=only,
                                         skip=skip))
        finally:
            delattr(self, '_in_bootstrap')
            if hasattr(self, '_meta'):
                delattr(self, '_meta')

    def _bootstrap(self, meta, *,
                   parents=False,
                   fetch_data=False,
                   size_limit_mb=2,
                   recursive=False,
                   only=tuple(),
                   skip=tuple()):
        """ The actual bootstrap implementation """

        # figure out if we are actually bootstrapping this class or skipping it
        if not meta or meta.id is None:
            raise exc.BootstrappingError(f'PathMeta to bootstrap from has no id! {meta}')

        if only or skip:
            if meta.id.startswith('N:organization:'):  # FIXME :/
                # since we only go one organization at a time right now
                # we never want to skip the top level id
                log.info(f'Bootstrapping {meta.id} -> {self.local!r}')
            elif meta.id in skip:
                log.info(f'Skipped       {meta.id} since it is in skip')
                return
            elif only and meta.id not in only:
                log.info(f'Skipped       {meta.id} since it is not in only')
                return
            else:
                # if you pass the only mask so do your children
                log.info(f'Bootstrapping {meta.id} -> {self.local!r}')
                only = tuple()

        if self.meta is not None and not recursive:
            raise exc.BootstrappingError(f'{self} already has meta!\n{self.meta.as_pretty()}')

        if self.exists() and self.meta and self.meta.id == meta.id:
            self._meta_updater(meta)

        else:
            # set single use bootstrapping id
            self._bootstrapping_id = meta.id

            # directory, file, or fake file as symlink?
            is_file_and_fetch_data = self._bootstrap_prepare_filesystem(parents,
                                                                        fetch_data,
                                                                        size_limit_mb)

            #self.meta = self.remote.meta
            self._bootstrap_data(is_file_and_fetch_data)

        if recursive:  # ah the irony of using loops to do this
            yield from self._bootstrap_recursive(only, skip)

        yield self

    def _bootstrap_recursive(self, only=tuple(), skip=tuple()):
        # TODO if rchildren looks like it could be bad
        # go back up to dataset level?
        sname = lambda gen: sorted(gen, key=lambda c: c.name)
        rcs = sname(self.remote._rchildren(create_cache=False))
        local_paths = list(self.local.rchildren)
        local_files = set(p for p in local_paths if p.is_file() or p.is_broken_symlink())
        local_dirs = set(p.relative_to(self.anchor) for p in local_paths if p.is_dir())
        if local_dirs:
            remote_dirs = set(c for c in rcs if c.is_dir())
            rd = set(d.as_path() for d in remote_dirs)
            old_local = local_dirs - rd
            while old_local:
                thisl = sorted(old_local, key=lambda d: len(d.as_posix()))
                for d in thisl:
                    ad = self.anchor.local / d
                    if ad.cache is None:
                        log.critical(f'would you fix the nullability already? {d}')
                        continue
                    new = ad.cache.refresh()
                    #log.info(f'{new}')
                    local_dirs = set(ld for ld in local_dirs
                                    if not ld.as_posix().startswith(d.as_posix()))
                    old_local = local_dirs - rd

        file_index = {f.cache.id:f for f in local_files}  # FIXME WARNING can get big
        for child in sorted(rcs, key=lambda c: len(c.as_path().as_posix())):
            # use the remote's recursive implementation
            # not the local implementation, since the
            # remote may have additional requirements
            #child.bootstrap(only=only, skip=skip)
            # because of how remote works now we don't even have to
            # bootstrap this
            cc = child.cache
            if cc is None:
                if child.is_file() and child.id in file_index:
                    _cache = file_index[child.id].cache
                    cmeta = _cache.meta
                    rmeta = child.meta
                    file_is_different, nmeta = self._update_meta(cmeta, rmeta)
                    if file_is_different:
                        log.critical(f'WAT {_cache}')
                    else:
                        yield _cache
                        # yield the old cache if it exists
                        # otherwise consumers of bootstrap will
                        # think the file may have been deleted
                        continue
                    
                cc = child.cache_init()

            yield cc

    def _bootstrap_prepare_filesystem(self, parents, fetch_data, size_limit_mb):
        # we could use bootstrapping id here and introspect the id, but that is cheating
        if self.remote.is_dir():
            if not self.exists():
                # the bug where this if statement put in as an and is a really
                # good example of how case/cond etc help you reasona about what
                # a block of branches is really doing -- this one was implementing
                # a covering set which is not obvious if implemented this way
                # you could do this with a dict or something else in pythong
                # bit it is awkward (see also my crazy case implementation in interlex)
                self.mkdir(parents=parents)

        elif self.remote.is_file():
            if not self.parent.exists():
                self.parent.mkdir(parents=parents)

            toucha_da_filey = (fetch_data and
                               self.meta.size is not None and
                               self.meta.size.mb < size_limit_mb)

            if toucha_da_filey:
                self.touch()
                # running this first means that we will use xattrs instead of symlinks
                # this is a bit opaque, but since meta uses a setter we can't pass a
                # param to make it clear (oh look, python being dumb again!)
            else:
                pass  # we are using symlinks

        else:
            raise BaseException(f'Remote is not a file or directory {self}')

    def _bootstrap_data(self, is_file_and_fetch_data):
        if is_file_and_fetch_data:
            if self.remote.meta.size is None:
                self.remote.refresh(update_cache=True)

            self.local.data = self.remote.data
            # with open -> write should not cause the inode to change

            self.validate_file()

    def validate_file(self):
        meta = self.meta
        if meta.etag:
            local_checksum, local_count = self.local.etag(meta.chunksize)
            cache_checksum, cache_count = meta.etag
            if local_checksum != cache_checksum or local_count != cache_count:
                msg = (f'etags do not match!\n(!='
                       f'\n{local_checksum}-{local_count}'
                       f'\n{cache_checksum}-{cache_count}\n)')
                log.critical(msg)

        elif meta.checksum:
            lc = self.local.meta.checksum 
            cc = self.meta.checksum
            if lc != cc:
                msg = f'Checksums do not match!\n(!=\n{lc}\n{cc}\n)'
                log.critical(msg)  # haven't figured out how to comput the bf checksums yet
                #raise exc.ChecksumError(msg)
        elif meta.size is not None:
            log.warning(f'No checksum! Your data is at risk!\n'
                        f'{self.remote!r} -> {self.local!r}! ')
            ls = self.local.meta.size
            cs = self.meta.size
            if ls != cs:
                raise exc.SizeError(f'Sizes do not match!\n(!=\n{ls}\n{cs}\n)')
        else:
            log.warning(f'No checksum and no size! Your data is at risk!\n'
                        '{self.remote!r} -> {self.local!r}! ')

    @property
    def remote(self):
        if hasattr(self, '_remote'):
            return self._remote

        if hasattr(self, '_cache_parent'):
            return self._cache_parent.remote

        id = self.id  # bootstrapping id is a one time use so keep it safe
        if id is None:  # zero is a legitimate identifier
            return

        anchor = self.anchor
        if anchor is None:  # the very first ...
            # in which case we need the id for factory AND class
            self._bootstrapping_id = id  # so we set it again
            anchor = self  # could double check if the id has the info too ...

        if self._remote_class_factory is not None or (hasattr(self, '_remote_class') and
                                                      self._remote_class is not None):
            # we don't have to have a remote configured to check the cache
            if not hasattr(self, '_remote_class'):
                #log.debug('rc')
                # NOTE there are many possible ways to set the anchor
                # we need to pick _one_ of them
                self._remote_class = self._remote_class_factory(anchor,
                                                                self.local_class)

            if not hasattr(self, '_remote'):
                self._remote = self._remote_class(id, cache=self)

            return self._remote

    @property
    def local(self):
        local = self.local_class(self)
        if self.is_helper_cache:
            cache = self._cache_parent 
        else:
            cache = self

        local._cache = cache
        return local

    def dedupe(self, other, pretend=False):
        # FIXME blackfynn doesn't set update when a folder name changes ??!
        if self.id != other.id:
            raise ValueError('Can only dedupe when ids match, {self.id} != {other.id}')

        su, ou = self.meta.updated, other.meta.updated
        lsu, lou = self.local.meta.updated, other.local.meta.updated
        if su < ou:
            old, new = self, other

        elif su > ou:
            new, old = self, other

        elif lsu is None and lou is None:
            new, old = self, other

        elif lsu is None:
            old, new = self, other

        elif lou is None:
            new, old = self, other

        elif lsu < lou:
            old, new = self, other

        elif lsu > lou:
            new, old = self, other

        else:  # ==
            ss, os = self.meta.size, other.meta.size
            if ss is not None and os is not None:
                new, old = self, other

            elif ss is None:
                old, new = self, other

            elif os is None:
                new, old = self, other

            else:
                raise BaseException('how did we get here!?')

        file_is_different, meta = self._update_meta(old.meta, new.meta)
        if file_is_different:
            log.info(f'{self}\n!=\n{other}\n{meta}')

        if not pretend:
            #old.rename('/dev/null')  # hah
            pass

        return new
        # TODO go look in meta for this
        # check updated ... etc.
        # missing size
        # missing file_id

    @property
    def id(self):
        if not hasattr(self, '_id'):  # calls to self.exists() are too expensive for this nonsense
            if self.meta:
                self._id = self.meta.id
                return self._id

            elif hasattr(self, '_bootstrapping_id'):
                id = self._bootstrapping_id
                delattr(self, '_bootstrapping_id')  # single use only
                return id
            else:
                return

        return self._id

    @property
    def cache_key(self):
        """ since some systems have compound ids ... """
        raise NotImplementedError

    # TODO how to toggle fetch from remote to heal?
    @property
    def meta(self):
        raise NotImplementedError

        if hasattr(self, '_meta'):
            return self._meta  # for bootstrap

    def _meta_setter(self, pathmeta, memory_only=False):
        """ so much for the pythonic way when the language won't even let you """
        if not memory_only:
            raise TypeError('You must explicitly set memory_only=True to use this '
                            'otherwise you risk dataloss.')

        if self.meta and self.id != pathmeta.id:
            raise exc.MetadataIdMismatchError('Cache id does not match meta id! '
                                              f'{self.id} != {pathmeta.id}\n{pathmeta}')

        self._meta = pathmeta

    def recover_meta(self):
        """ rebuild restore reconnect """

        root = self.parent.local.find_cache_root()
        if root is None:
            #breakpoint()
            raise exc.NotInProjectError(f'{self.parent.local} is not in a project!')
        breakpoint()
        raise NotImplementedError()
        children = list(self.parent.remote.children)  # if this is run from dismatch meta we have issues
        isf = self.is_file()
        isd = self.is_dir()
        candidates = []
        def inner(child):
            if child.is_dir() and isd:
                if child.name == self.name:
                    self.meta = child.meta
                    return

            elif child.is_file() and isf:
                log.debug(f'{child.name} {child.stem}, {child.suffix!r}')
                log.debug(f'{self.name} {self.stem}, {self.suffix!r}')
                if child.name == self.name:
                    self.meta = child.meta
                elif child.name == self.stem:
                    candidates.append(child)
                elif child.stem == self.name:
                    candidates.append(child)
                elif child.stem == self.stem:
                    # worst cases
                    candidates.append(child)

            else:
                #log.critical('file type mismatch')
                pass

        for child in children:
            inner(child)
            # it looks like if we do fail over to retrieving a package it does go to files
            # so this is an ok approach and we don't have to deal with that at this level
        if not candidates:
            wat = '\n'.join(c.name for c in children)
            message = (f'We seem to have lost {self.parent} -/-> {self.name}'
                       f'\n{self.parent.uri_human}\n{wat}\n{self.name}')
            log.critical(message)
            dataset = self.dataset
            maybe = []
            for c in self.dataset.remote.rchildren:
                if c.parent and c.parent.id == self.parent.id or c.stem == self.stem:
                    maybe.append(c)

            [inner(m) for m in maybe]
            #candidates
            #dataset.bootstrap(dataset.meta, recursive=True)
            #raise exc.NoRemoteMappingError

        elif len(candidates) == 1:
            remote = candidates[0]
            log.critical('How did we write this file without moving it beforhand?!?\n'
                         f'{self.local} -/-> {remote.name}')
            self.meta = remote.meta  # go ahead and set this even though we change?
            self.move(remote=remote)
        else:
            raise BaseException('multiple candidates!')

    def refresh(self, update_data=False, size_limit_mb=2, force=False):
        if self.meta is None:
            breakpoint()
        limit = (size_limit_mb if
                 not self.meta.size or (size_limit_mb > self.meta.size.mb)
                 else self.meta.size.mb + 1)
        new = self.remote.refresh(update_cache=True,
                                  update_data=update_data,
                                  update_data_on_cache=(self.is_file() and self.exists()),
                                  size_limit_mb=size_limit_mb,
                                  force=force)
        if new is not None:
            return new
        else:
            log.info(f'Remote for {self} has been deleted. Moving to trash.')
            try:
                self.rename(self.trash / f'{self.parent.id}-{self.id}-{self.name}')
            except FileNotFoundError as e:
                if not self.trash.exists():
                    self.trash.mkdir()
                    log.info('created {self.trash}')
                else:
                    raise e

    def fetch(self, size_limit_mb=2):
        """ bypass remote to fetch directly based on stored meta """
        meta = self.meta
        if self.is_dir():
            raise NotImplementedError('not going to fetch all data in a dir at the moment')
        if meta.file_id is None:
            self.refresh(update_data=True, force=True)
            # the file name could be different so we have to return here
            return

        size_ok = size_limit_mb is not None and meta.size is not None and meta.size.mb < size_limit_mb
        size_not_ok = size_limit_mb is not None and meta.size is not None and meta.size.mb > size_limit_mb

        if size_ok or size_limit_mb is None:  # FIXME should we force fetch here by default if the file exists?
            if self.is_broken_symlink():
                # FIXME touch a temporary file and set the meta first!
                self.unlink()
                self.touch()
                self._meta_setter(meta)

            log.info(f'Fetching from cache id {self.id} -> {self.local}')
            self.local.data = self.data  # note that this should trigger storage to .ops/objects

        if size_not_ok:
            log.warning(f'File is over the size limit {meta.size.mb} > {size_limit_mb}')

    def move(self, *, remote=None, target=None, meta=None):
        """ instantiate a new cache and cleanup self because we are moving """
        # FIXME what to do if we have data
        if remote is None and (target is None or meta is None):
            raise TypeError('either remote or meta and target are required arguments')

        # deal with moving to a different directory that might not even exist yet
        if target is None:
            if not isinstance(self.anchor, self.__class__):
                raise TypeError('mismatched anchor types {self!r} {self.anchor!r}')

            target = self.anchor / remote  # FIXME why does this not try to instantiate the caches? or does it?

        if target.absolute() == self.absolute():
            log.warning(f'trying to move a file onto itself {self.absolute()}')
            return target

        common = self.commonpath(target).absolute()
        parent = self.parent.absolute()

        assert target.name != self.name or common != parent

        if common != parent:
            _id = remote.id if remote else meta.id
            log.warning('A parent of current file has changed location!\n'
                        f'{common}\n{self.relative_to(common)}\n'
                        f'{target.relative_to(common)}\n{_id}')


        if not target.parent.exists():
            if remote is None:  # we have to have a remote to pull parent structure
                remote = self._remote_class(meta)

            target.parent.mkdir_cache(remote)

        if not isinstance(target, self.__class__):
            target = self.__class__(target, meta=meta)

        if target.exists() or target.is_broken_symlink():
            if target.id == self.id: #(remote.id if remote else meta.id):
                if self.is_broken_symlink():
                    # we may be a package with extra metadata that needs to
                    # be merged with the target before we are unlinked
                    file_is_different = target._meta_updater(self.meta)
                    # FIXME ... if file is different then this causes staleness
                    # and we need to fetch
                    if file_is_different:
                        log.critical('DO SOMETHING ABOUT THIS STALE DATA'
                                     f'\n{target}\n{target.meta.as_pretty()}')
                else:
                    # directory moves that are resolved during pull
                    log.warning(f'what is this!?\n{target}\n{self}')
            elif target.is_broken_symlink():
                remote._cache = self  # restore the mapping for remote -> self
                raise exc.WhyDidntThisGetMovedBeforeError(f'\n{target}\n{self}')
            else:
                raise exc.PathExistsError(f'Target {target} already exists!')

        if self.exists():
            self.rename(target)  # if target is_dir then this will fail, which is ok

        elif self.is_broken_symlink():
            # we don't move to trash here because this was just a file rename
            self.unlink()  # don't move the meta since it will break the naming insurance measure

        return target

    def __repr__(self):
        local = repr(self.local) if self.local else 'No local??' + str(self)
        remote = (f'{self.remote.__class__.__name__}({self.id!r})'
                  if self.remote else str(self.id))
        return self.__class__.__name__ + ' <' + local + ' -> ' + remote + '>'


class XattrPath(AugmentedPath):
    """ pathlib Path augmented with xattr support """

    def setxattr(self, key, value, namespace=xattr.NS_USER):
        if not isinstance(value, bytes):  # checksums
            raise TypeError(f'setxattr only accepts values already encoded to bytes!\n{value!r}')
        else:
            bytes_value = value

        xattr.set(self.as_posix(), key, bytes_value, namespace=namespace)

    def setxattrs(self, xattr_dict, namespace=xattr.NS_USER):
        for k, v in xattr_dict.items():
            self.setxattr(k, v, namespace=namespace)

    def getxattr(self, key, namespace=xattr.NS_USER):
        # we don't deal with types here, we just act as a dumb store
        return xattr.get(self.as_posix(), key, namespace=namespace)

    def xattrs(self, namespace=xattr.NS_USER):
        # decode keys later
        try:
            return {k:v for k, v in xattr.get_all(self.as_posix(), namespace=namespace)}
        except FileNotFoundError as e:
            raise FileNotFoundError(self) from e


class ReflectiveCachePath(CachePath):
    """ Oh, it's me. """

    @property
    def meta(self):
        return self.local.meta


class XattrCache(CachePath, XattrPath):
    xattr_prefix = None

    @property
    def meta(self):
        if self.exists():
            xattrs = self.xattrs()
            pathmeta = PathMeta.from_xattrs(xattrs, self.xattr_prefix, self)
            return pathmeta

    def _meta_setter(self, pathmeta, memory_only=False):
        #log.warning(f'!!!!!!!!!!!!!!!!!!!!!!!!2 {self}')
        # TODO cooperatively setting multiple different cache types?
        # do we need to use super() or something?

        if self.exists():
            if self.is_symlink():
                raise TypeError('will not write meta on symlinks! {self}')
            # FIXME FIXME FIXME this needs to be written to absolutely
            # prevent the writing of new metadata onto an existing file
            # where the checksum differs, the old version needs to be
            # trashed before any of this is written, otherwise the old
            # metadata is lost >_<
            self.setxattrs(pathmeta.as_xattrs(self.xattr_prefix))
            if hasattr(self, '_meta'):  # prevent reading from in-memory store
                delattr(self, '_meta')

        else:
            # the glories of the inconsistencies and irreglarities of python
            # you can't setattr using super() so yes you _do_ actually have to
            # implement a setter sometimes >_<
            super()._meta_setter(pathmeta, memory_only=memory_only)


class SqliteCache(CachePath):
    """ a persistent store to back up the xattrs if they get wiped """

    def __init__(self, *args, meta=None, **kwargs):
        if meta is not None:
            self.meta = meta

    @property
    def meta(self):
        if hasattr(self, '_meta'):
            return self._meta

        #log.error('SqliteCache getter not implemented yet.')

    @meta.setter
    def meta(self, value):
        """ set meta """
        #log.error('SqliteCache setter not implemented yet. Should probably be done in bulk anyway ...')


class SymlinkCache(CachePath):

    def __init__(self, *args, meta=None, **kwargs):
        if meta is not None:
            self.meta = meta

    @property
    def meta(self):
        if hasattr(self, '_meta'):
            return self._meta

        if self.is_symlink():
            if not self.exists():  # if a symlink exists it is something other than what we want
                #assert PurePosixPath(self.name) == self.readlink().parent.parent
                return PathMeta.from_symlink(self)
            else:
                raise exc.PathExistsError(f'Target of symlink exists!\n{self} -> {self.resolve()}')

        else:
            return super().meta

    @meta.setter
    def meta(self, pathmeta):
        if not self.exists():
            # if the path does not exist write even temporary to disk
            if self.is_symlink():
                meta = self.meta
                if meta == pathmeta:
                    log.debug(f'Metadata unchanged for {meta.id}. Not updating.')
                    return

                if meta.id != pathmeta.id:
                    msg = ('Existing cache id does not match new id!\n'
                           f'{self!r}\n'
                           f'{meta.id} != {pathmeta.id}\n'
                           f'{meta.as_pretty()}\n'
                           f'{pathmeta.as_pretty()}')
                    log.critical(msg)
                    meta_newer = 'Meta newer. Not updating.'
                    pathmeta_newer = 'Other meta newer.'
                    msg = '{}'  # apparently I was out of my mind when I wrote this originally ...
                    if meta.updated > pathmeta.updated:
                        log.info(msg.format(meta_newer))
                        return  # this is the right thing to do for a sane filesystem
                    elif meta.updated < pathmeta.updated:
                        log.info(msg.format(pathmeta_newer))
                    else:  # they are equal
                        extra = 'Both updated at the same time ' 
                        if meta.created is not None and pathmeta.created is not None:
                            if meta.created > pathmeta.created:
                                log.info(msg.format(extra + meta_newer))
                                return
                            elif meta.created < pathmeta.created:
                                log.info(msg.format(extra + pathmeta_newer))
                            else:  # same created
                                log.info(msg.format('Identical timestamps. Not updating.'))
                                return
                        elif meta.created is not None:
                            log.info(msg.format(extra + 'Meta has datetime other does not. Not updating.'))
                            return
                        elif pathmeta.created is not None:
                            log.info(msg.format(extra + 'Meta has no datetime other does.'))
                        else:  # both none
                            log.info(msg.format(extra + ('Identical update time both missing created time. '
                                                         'Not updating.')))
                            return
                    #raise exc.MetadataIdMismatchError(msg)
                    return

                if meta.size is not None and pathmeta.size is None:
                    log.error('new meta has no size so will not overwrite')
                    return

                # FIXME do the timestamp dance above here
                log.debug('Metadata exists, but ids match so will update')

                # trash old versions instead of just unlinking
                pc = self.local.cache
                trash = pc.trash
                self.rename(trash / f'{pc.parent.id}-{meta.id}-{self.name}')
                #self.unlink()

            # FIXME if an id starts with / then the local name is overwritten due to pathlib logic
            # we need to error if that happens
            #symlink = PurePosixPath(self.local.name, pathmeta.as_symlink().as_posix().strip('/'))
            symlink = PurePosixPath(self.local.name) / pathmeta.as_symlink()
            self.local.symlink_to(symlink)

        else:
            raise exc.PathExistsError(f'Path exists {self}')


class PrimaryCache(CachePath):
    @property
    def meta(self):
        #if hasattr(self, '_in_bootstrap'):
        #if hasattr(self, '_meta'):  # if we have in memory we are bootstrapping so don't fiddle about
            #return self._meta

        exists = self.exists()
        if exists:
            #log.debug(self)  # TODO this still gets hit a lot in threes
            meta = super().meta
            if meta:  # implicit else failover to backup cache
                return meta

        elif not exists and self._not_exists_cache and self.is_symlink():
            try:
                cache = self._not_exists_cache(self)
                return cache.meta
            except exc.NoCachedMetadataError as e:
                log.warning(e)

        if self._backup_cache:
            try:
                cache = self._backup_cache(self)
                meta = cache.meta
                if meta:
                    log.info(f'restoring from backup {meta}')
                    self._meta_setter(meta)  # repopulate primary cache from backup
                    return meta

            except exc.NoCachedMetadataError as e:
                log.warning(e)

    def _meta_setter(self, pathmeta, memory_only=False):
        """ we need memory_only for bootstrap I think """
        if not pathmeta:
            log.warning(f'Trying to set empty pathmeta on {self}')
            return

        if self.exists_not_symlink():  # if a file already exists just follow instructions
            super()._meta_setter(pathmeta)

        else:
            if not hasattr(self, '_remote') or self._remote is None:
                self._bootstrapping_id = pathmeta.id

            # need to run this to create directories
            self._bootstrap_prepare_filesystem(parents=False, fetch_data=False, size_limit_mb=0)

            if self.exists():  # we a directory now
                super()._meta_setter(pathmeta)

            elif self._not_exists_cache:
                cache = self._not_exists_cache(self, meta=pathmeta)

        if self._backup_cache:
            cache = self._backup_cache(self, meta=pathmeta)

        if hasattr(self, '_meta'):
            delattr(self, '_meta')

        if hasattr(self, '_id'):
            delattr(self, '_id')

    @staticmethod
    def _update_meta(old, new):
        if not old:
            return False, new  # if there is no file it is both different and not different

        if not new:
            return False, old

        file_is_different = False

        kwargs = {k:v for k, v in old.items()}
        if old.id != new.id:
            kwargs['old_id'] = old.id

        for k, vnew in new.items():
            vold = kwargs[k]

            if vnew is None or hasattr(vnew, '__iter__') and not vnew:
                # don't update with None or empty iterables
                continue

            if vold is not None and vold != vnew:
                log.info(f'{old.id} field {k} changed from {vold} -> {vnew}')
                if k in ('created', 'updated', 'size', 'checksum', 'file_id'):
                    file_is_different = True

            kwargs[k] = vnew

        if file_is_different:
            # strip fields missing from new in the case where
            # we aren't merging metadata from two different sources

            for k, vnew in new.items():
                if k == 'old_id':
                    continue

                if vnew is None:
                    log.debug(kwargs.pop(k))

        #old.updated == new.updated
        #old.updated < new.updated
        #old.updated > new.updated

        #old.created == new.created
        #old.created < new.created
        #old.created > new.created

        return file_is_different, PathMeta(**kwargs)

    def _meta_updater(self, pathmeta):
        original = self.meta
        file_is_different, updated = self._update_meta(original, pathmeta)
        must_fetch = file_is_different and self.is_file() and self.exists()

        if must_fetch:
            try:
                # FIXME performance, and pathmeta.checksum is None case
                if self.local.content_different() and self.local.meta.checksum != pathmeta.checksum:
                    raise exc.LocalChangesError(f'not fetching {self}')

            except exc.NoRemoteFileWithThatIdError as e:
                log.warning('cant fetch remote file there may be untracked local changes for\n{self}')

            log.info(f'crumpling to preserve existing metadata\n{self}')
            trashed = self.crumple()

        try:
            self._meta_setter(updated)
            if must_fetch:
                self.fetch(size_limit_mb=None)

        except BaseException as e:
            log.error(e)
            if must_fetch:
                trashed.rename(self)
            raise e

        return file_is_different

    @property
    def children(self):
        """ direct children """
        # if you want the local children go on local
        # this will give us the remote children in the local context
        # going in the reverse direction with parents
        # we don't do because the parents here are already defined
        # if a file has moved on the remote we can detect that and error for now
        for child_remote in self.remote.children:
            child_cache = self / child_remote
            yield child_cache

    @property
    def rchildren(self):
        # FIXME cached rchildren vs local (working tree) rchildren vs remote rchildren
        # have to express the generator to build the index
        # otherwise child.parents will not work correctly (annoying)
        for child_remote in self.remote.rchildren:
            # FIXME is this causing the creation of folders during bootstrap ???
            args = child_remote._parts_relative_to(self.remote)  # usually this would just be one level
            child_cache = self._make_child(args, child_remote)
            #child_cache = self
            #child_path = self.__class__(self, *args)
            #child_path.remote = child

            yield child_cache

        # if organization
        # if dataset (usually going to be the fastest in most cases)
        # if collection (can end up very slow)
        # if package/file


class SshCache(PrimaryCache, XattrCache):
    xattr_prefix = 'ssh'
    _backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache

    @property
    def anchor(self):
        if not hasattr(self, '_anchor') or self._anchor is None:
            raise ValueError('Cache anchor is none! Did you call '
                             'localpath.cache_init(id, anchor=True)?')

        return self._anchor

    @property
    def data(self):
        # there is no middle man for ssh so we go directly
        yield from self.remote.data


class LocalPath(XattrPath):
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
            self._cache_class._anchor = cache

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
        for variant in set((self, self.absolute(), self.resolve())):
            for parent in chain((variant,), variant.parents):
                try:
                    if parent.cache:
                        found_cache = parent
                except (exc.NoCachedMetadataError, exc.NotInProjectError) as e:
                    # if we had a cache, went to the parent and lost it
                    # then we are at the root, assuming of course that
                    # there aren't sparse caches on the way up (down?) the tree
                    if found_cache is not None and found_cache != Path('/'):
                        return found_cache

            else:
                if found_cache and found_cache != Path('/'):
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
            return (rel_path._cpath[0] in self._cache_class.cache_ignore or
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
                # implemented this way we can still use Path to navigate
                # once we are inside local data dir, though all files there
                # are skip_cache -> True
                for path in self.iterdir():
                    if path.stem == LOCAL_DATA_DIR:
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


class RepoPath(PosixPath):
    _repos = {}  # repo cache

    @property
    def repo(self):
        if self in self._repos:
            return self._repos[self]

        elif (self / '.git').exists():  # FIXME kind of a hack
            repo = Repo(self.as_posix())
            self._repos[self] = repo
            return repo

        elif str(self) == '/':  # FIXME windows ...
            return None

        else:
            return self.parent.repo

    @property
    def repo_relative_path(self):
        """ working directory relative path """
        repo = self.repo
        if repo is not None:
            return self.relative_to(repo.working_dir)


class XopenPath(PosixPath):

    def xopen(self):
        """ open file using xdg-open """
        process = subprocess.Popen(['xdg-open', self.as_posix()],
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


# assign defaults

SshCache._local_class = LocalPath

# any additional values
LocalPath._bind_sysid()
