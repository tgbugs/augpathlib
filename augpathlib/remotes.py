import os
import sys
import atexit
import pathlib
import warnings
import subprocess
from augpathlib import exceptions as exc
from augpathlib.meta import PathMeta
from augpathlib import caches, LocalPath
from augpathlib.utils import _bind_sysid_, StatResult, cypher_command_lookup, log
if os.name != 'nt':
    # pexpect on windows does not support pxssh
    # because it is missing spawn
    from pexpect import pxssh


class RemotePath:
    """ Remote data about a remote object. """

    _cache_class = None
    _debug = False

    # ugh this is such a bad implementation, let the remote paths exists
    # and init, and then just check if they exist, a path is not an object
    # that always dereferences ... what the heck was I thinking when I did this ...

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
        """ when constructing a new remote using _new you MUST
            call init afterward to bind the remote api """

        # FIXME 1:1ness issue from local -> cache
        # probably best to force the type of the cache
        # to switch if there are multiple remote mappings
        # since there can be only 1 local file with the same
        # path, a composite cache or a multi-remote cache
        # seems a bit saner, or require explicit switching of
        # the active remote if one-at-a-time semantics are desired

        newcls = type(cls.__name__,
                      (cls,),
                      dict(_local_class=local_class,
                           _cache_class=cache_class))

        local_class._remote_class = newcls
        local_class._cache_class = cache_class
        cache_class._remote_class = newcls
        cache_class._local_class = local_class

        newcls.weighAnchor()
        cache_class.weighAnchor()

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
    def anchorToCache(cls, cache_anchor, init=True):
        # FIXME need to check for anchor after init and init after anchor
        if not hasattr(cls, '_cache_anchor'):
            if init:
                if not hasattr(cls, '_api'):
                    cls.init(cache_anchor.id)

            if hasattr(cls, 'root') and cls.root != cache_anchor.id:
                raise ValueError('root and anchor ids do not match! '
                                 f'{cls.root} != {cache_anchor.id}')

            cls._cache_anchor = cache_anchor
            return cls._cache_anchor
        else:
            raise ValueError(f'already anchored to {cls._cache_anchor}')

    @classmethod
    def anchorTo(cls, path, create=False):
        """ You already know the rock you want and
            you want the anchor stuck to it. """
        # FIXME should we fail on create=True and exists?

        if isinstance(path, caches.CachePath):
            # FIXME the non-existence problem rears its head again
            return cls.anchorToCache(path)
        elif isinstance(path, LocalPath):
            # FIXME the non-existence problem rears its head again
            if path.cache:
                return cls.anchorToCache(path.cache)
            else:
                root = cls.root if isinstance(cls.root, cls) else cls(cls.root)
                if path.name != root.name:
                    # unlike git you cannot clone to a folder with a different
                    # name (for now ... maybe can figure out how in the future)
                    raise ValueError('Path name and root name do not match.'
                                     f'{path.name} != {cls.root.name}')

                if create:
                    return cls.dropAnchor(path.parent)  # existing folder dealt with in dropAnchor
                else:
                    raise ValueError(f'not creating {path} since create=False')
        else:
            raise TypeError(f"Don't know how to anchor to a {type(path)} {path}")

    @classmethod
    def _get_local_root_path(cls, parent_path=None):
        if parent_path is None:
            parent_path = cls._local_class.cwd()
        else:
            parent_path = cls._local_class(parent_path)

        root = cls(cls.root)  # FIXME formalize the use of root
        path = parent_path / root.name
        return root, path

    @classmethod
    def smartAnchor(cls, parent_path=None):
        # work around the suspect logic
        # in the implementation below
        try:
            return cls.dropAnchor(parent_path=parent_path)
        except exc.RemoteAlreadyAnchoredError as e:
            root, path = cls._get_local_root_path(parent_path)
            if cls._cache_anchor == path.cache:
                return cls._cache_anchor
            else:
                raise e  # possibly check if the anchor is the same?

        except exc.CacheExistsError as e:
            root, path = cls._get_local_root_path(parent_path)
            cls._cache_anchor = path.cache
            return cls._cache_anchor

        except exc.DirectoryNotEmptyError as e:
            root, path = cls._get_local_root_path(parent_path)
            if path.cache:
                cls._cache_anchor = path.cache
                return cls._cache_anchor
            else:
                raise e

    @classmethod
    def dropAnchor(cls, parent_path=None):
        """ If a _cache_anchor does not exist then create it,
            otherwise raise an error. If a local anchor already
            exists do not use this method.

            You know that the ship (path) is more or less in the right
            place but you don't know for sure exactly which rock the
            anchor will catch on (you don't know the name of the remote).
        """
        if not hasattr(cls, '_cache_anchor'):
            root, path = cls._get_local_root_path(parent_path)
            if not path.exists():
                if root.is_file():
                    raise NotImplementedError(
                        'Have not implemented mapping for individual files yet.')

                elif root.is_dir():
                    path.mkdir()

                else:
                    raise NotImplementedError(f'What\'s a {root}?!')

            elif list(path.children):
                raise exc.DirectoryNotEmptyError(f'has children {path}')

            cls._cache_anchor = path.cache_init(root.id, anchor=True)
            # we explicitly do not handle the possible CacheExistsError here
            # so that there is a path where anchoring can fail loudly
            # we may not need that at the end of the day, but we will see
            return cls._cache_anchor

        else:
            raise exc.RemoteAlreadyAnchoredError(f'{cls} already anchored to '
                                                 f'{cls._cache_anchor}')

    @classmethod
    def weighAnchor(cls):
        # TODO determine whether the current behavior is correct
        # calling this will not cause the cache class to weigh anchor
        # but there is a small chance that it should

        # TODO is _abstract_class needed here? or do we not need it
        # because remote paths don't have the crazy hierarchy that
        # pathlib derived paths do? and will this change when we fix
        # everything ...

        if hasattr(cls, '_cache_anchor'):
            delattr(cls, '_cache_anchor')

    @classmethod
    def setup(cls, local_class, cache_class):
        """ call this once to bind everything together """

        cn = self.__class__.__name__
        warnings.warn(f'{cn}.setup is deprecated please switch to RemotePath._new',
                      DeprecationWarning,
                      stacklevel=2)

        cache_class.setup(local_class, cls)

    def bootstrap(self, recursive=False, only=tuple(), skip=tuple(), sparse=tuple()):
        #self.cache.remote = self  # duh
        # if you forget to tell the cache you exist of course it will go to
        # the internet to look for you, it isn't quite smart enough and
        # we're trying not to throw dicts around willy nilly here ...
        return self.cache.bootstrap(self.meta, recursive=recursive, only=only, skip=skip, sparse=sparse)

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
                def local(self, remote=self):
                    raise TypeError(f'No cache for {remote}')

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

    def cache_init(self, parents=False):
        try:
            return self._cache_anchor / self
        except FileNotFoundError:
            if parents:
                #parent, *rest = self.parent.cache_init(parents=parents)
                #return (self.cache_init(), parent, *rest)
                parent = self.parent
                parent_cache = parent.cache_init(parents=parents)
                parent_cache.local.cache_init(parent.meta) # FIXME hrm we shouldn't have to do this
                # and it isn't working anyway ... the xattrs don't seem to be getting set
                return self.cache_init()
            else:
                raise

    @property
    def _cache(self):
        """ To catch a bad call to set ... """
        if hasattr(self, '_c_cache'):
            return self._c_cache

    @_cache.setter
    def _cache(self, cache):
        if not isinstance(cache, caches.CachePath):
            raise TypeError(f'cache is a {type(cache)} not a CachePath!')
        #elif cache.meta is None:  # useful for certain debugging situations
            #raise ValueError(f'cache has no meta {cache}')

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
    def parent_id(self):
        """ BEWARE if self.parent hits the network so will this.
            In the event that it does, overwrite this method. """

        return self.parent.id

    def _parent_changed(self, cache):
        return self.parent_id != cache.parent.id

    def _on_cache_move_error(self, error, cache):
        """ called after a failure to move a cached file to a new location """
        raise error

    def update_cache(self, cache=None, fetch=True):
        """ Update a cache object using the metadata attached to this remote.

            This is different form _cache_setter in that it runs update_meta
            by default, handles many more edge cases, and checks for consistency.
            _cache_setter is usually invoked internally by a CachePath method that
            wants to register itself with a remote as an implementaiton detail. """

        if cache is not None and self.cache is not None:
            # TODO see if there are any exceptions to this behavior
            raise TypeError('cannot accept cache kwarg when self.cache not None')
        elif cache is None:
            cache = self.cache

        parent_changed = self._parent_changed(cache)

        if self.cache is None:
            # HACK test if cache is not None before it may have been reassigned
            if cache.name != self.name:
                msg = ('Cannot update the name and content of a file at the '
                       'same time.\nAre you sure you have passed the right '
                       f'cache object?\n{cache.name} != {self.name}')
                raise ValueError(msg)

            elif parent_changed:
                msg = ('Cannot update the parent and content of a file at the '
                       'same time.\nAre you sure you have passed the right '
                       f'cache object?\n{cache.parent.id} != {self.parent_id}')
                raise ValueError(msg)

        log.debug(f'maybe updating cache for {self.name}')
        file_is_different = cache._meta_updater(self.meta, fetch=fetch)
        # update the cache first  # FIXME this may be out of order ...
        # then move to the new name if relevant
        # prevents moving partial metadata onto existing files
        if cache.name != self.name or parent_changed:  # this is localy correct
            # the issue is that move is now smarter
            # and will detect if a parent path has changed
            try:
                cache.move(remote=self)
            except exc.WhyDidntThisGetMovedBeforeError as e:
                # AAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                # deal with the sadness that is non-unique filenames
                # I am 99.999999999999999% certain that users do not
                # expect this behavior ...
                log.error(e)
                self._on_cache_move_error(e, cache)

        return file_is_different

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
        return pathlib.PurePath(*self.parts)

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
        elif self == parent:
            args = ('',)
        else:
            args = self.name,

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

    def _rchildren(self, create_cache=True, sparse=False):
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


class SshRemote(RemotePath, pathlib.PurePath):
    """ Testing. To be used with ssh-agent.
        StuFiS The stupid file sync. """

    _cache_class = None  # set when calling __new__
    encoding = 'utf-8'

    _meta = None  # override RemotePath dragnet
    _meta_maker = LocalPath._meta_maker

    sysid = None
    _bind_sysid = classmethod(_bind_sysid_)

    @classmethod
    def _new(cls, local_class, cache_class):
        newcls = super()._new(local_class, cache_class)

        # must run before we can get the sysid, which is a bit odd
        # given that we don't actually sandbox the filesystem

        newcls._bind_flavours()
        return newcls

    @classmethod
    def _bind_flavours(cls, pos_helpers=tuple(), win_helpers=tuple()):
        pos, win = cls._get_flavours()

        if pos is None:
            pos = type(f'{cls.__name__}Posix',
                       (*pos_helpers, cls, pathlib.PurePosixPath), {})

        if win is None:
            win = type(f'{cls.__name__}Windows',
                       (*win_helpers, cls, pathlib.PureWindowsPath), {})

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

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_flavour'):
            cls = cls.__windowspath if os.name == 'nt' else cls.__posixpath

        if isinstance(args[0], str) and args[0].startswith(cls.host + ':'):
            # FIXME not great but allows less verbose where possible ...
            # also possibly an opportunity to check if hostnames match?
            # ugh unix everything is a stream of bytes is annoying here
            _, *args = (args[0].split(':', 1), *args[1:])

        _self = pathlib.PurePath.__new__(cls, *args)  # no kwargs since the only kwargs are for init
        _self.remote_platform = _self._remote_platform
        return _self
    
        # TODO this isn't quite working yet due to bootstrapping issues as usual
        # it also isn't working because we want access to all paths in many cases
        # the root remains and the calculation against anchor remains for any
        # relative path that is provided, and the default behavior for absolute
        # paths protects us from sillyness

        if _self.id != cls.root: #_cache_anchor.id:
            self = _self.relative_to(_self.anchor)
        else:
            self = pathlib.PurePath.__new__(cls, '.')  # FIXME make sure this is interpreted correctly ...

        self._errors = []
        return self

    @classmethod
    def init(cls, host_path):
        """ should only be invoked after _new has bound local and cache classes """
        if not hasattr(cls, '_anchor'):
            cls.root = host_path  # I think this is right ...
            host, path = host_path.split(':', 1)

            if not hasattr(cls, '_flavour'):
                cls = cls.__windowspath if os.name == 'nt' else cls.__posixpath

            cls._anchor = pathlib.PurePath.__new__(cls, path)

            session = pxssh.pxssh(options=dict(IdentityAgent=os.environ.get('SSH_AUTH_SOCK')))
            session.login(host, ssh_config=LocalPath('~/.ssh/config').expanduser().as_posix())
            cls._rows = 200
            cls._cols = 200
            session.setwinsize(cls._rows, cls._cols)  # prevent linewraps of long commands
            session.prompt()
            atexit.register(lambda:(session.sendeof(), session.close()))
            cls.host = host
            cls.session = session
            cls._ssh("bind 'set enable-bracketed-paste off'")
            cls._uid, *cls._gids = [int(i) for i in
                                    cls._ssh('echo $(id -u) $(id -G)')
                                    .decode().split(' ')]
        else:
            raise ValueError(f'{cls} already bound an remote to {cls._anchor}')

    @classmethod
    def anchorToCache(cls, cache_anchor, init=True):
        anchor = super().anchorToCache(cache_anchor=cache_anchor, init=init)
        # _cache_anchor has to be bound for _bind_sysid to work
        # that binding happens after init so we do this here
        cls._bind_sysid()
        return anchor

    def __init__(self, thing_with_id, cache=None):
        if isinstance(thing_with_id, pathlib.PurePath):
            thing_with_id = thing_with_id.as_posix()

        super().__init__(thing_with_id, cache=cache)

    @property
    def anchor(self):
        return self._anchor
        #return self._cache_anchor.remote
        # FIXME warning on relative paths ...
        # also ... might be convenient to allow
        # setting non-/ anchors, but perhaps for another day
        #return self.__class__('/', host=self.host)

    @property
    def id(self):
        return f'{self.host}:{self.rpath}'
        #return self.host + ':' + self.as_posix()  # FIXME relative to anchor?

    @property
    def cache_key(self):
        """ since some systems have compound ids ... """
        raise NotImplementedError

    @property
    def rpath(self):
        # FIXME relative paths when the anchor is set differently
        # the anchor will have to be stored as well since there coulde
        # be many possible anchors per host, thus, if an anchor relative
        # identifier is supplied then we need to construct the full path

        # conveniently in this case if self is a fully rooted path then
        # it will overwrite the anchor path
        # TODO make sure that the common path is the anchor ...
        return (self.anchor / self).as_posix()

    def _parts_relative_to(self, remote, cache_parent=None):
        if remote == self.anchor:
            # have to build from self.anchor._parts because it is the only
            # place the keeps the original parts
            remote = pathlib.PurePath(*self.anchor._parts)

        return self.relative_to(remote).parts

    def refresh(self):
        # TODO probably not the best idea ...
        raise NotImplementedError('This baby goes to the network every single time!')

    def access(self, mode):
        """ types are 'read', 'write', and 'execute' """
        try:
            st = self.stat()

        except (PermissionError, FileNotFoundError) as e:
            return False

        r, w, x = 0x124, 0x92, 0x49
        read    = ((r & st.st_mode) >> 2) & (mode == 'read'    or mode == os.R_OK) * x
        write   = ((w & st.st_mode) >> 1) & (mode == 'write'   or mode == os.W_OK) * x
        execute =  (x & st.st_mode)       & (mode == 'execute' or mode == os.X_OK) * x
        current = read + write + execute

        u, g, e = 0x40, 0x8, 0x1
        return (u & current and st.st_uid == self._uid or
                g & current and st.st_gid in self._gids or
                e & current)

    def open(self, mode='wt', buffering=-1, encoding=None,
             errors=None, newline=None):
        if mode not in ('wb', 'wt'):
            raise TypeError('only w[bt] mode is supported')  # TODO ...

        #breakpoint()
        return
        class Hrm:
            session = self.session
            def write(self, value):
                self.session

        #cmd = ['ssh', self.host, f'"cat - > {self.rpath}"']
        #self.session
        #p = subprocess.Popen()

    @property
    def data(self):
        cmd = ['ssh', self.host, f'cat {self.rpath!r}']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        while True:
            data = p.stdout.read(4096)  # TODO hinting
            if not data:
                break

            yield data

        p.communicate()

    # reuse meta from local
    # def meta (make it easier to search for this)
    meta = LocalPath.meta  # magic

    #def _ssh(self, remote_cmd):
    @classmethod
    def _ssh(cls, remote_cmd):
        #print(remote_cmd)
        if len(remote_cmd) > cls._cols:
            raise exc.CommandTooLongError
        n_bytes = cls.session.sendline(remote_cmd)
        cls.session.prompt()
        raw = cls.session.before
        out = raw[n_bytes + 1:].strip()  # strip once here since we always will
        #print(raw)
        #print(out)
        return out

    @property
    def _remote_platform(self):
        remote_cmd = "uname -a | awk '{ print tolower($1) }'"
        return self._ssh(remote_cmd).decode(self.encoding)

    @property
    def cypher_command(self):
        # this one is a little backwards, because we can control
        # whatever cypher we want, unlike in other cases
        return cypher_command_lookup[self._cache_class.cypher]

    def checksum(self):
        remote_cmd = (f'{self.cypher_command} {self.rpath} | '
                      'awk \'{ print $1 }\';')

        hex_ = self._ssh(remote_cmd).decode(self.encoding)
        log.debug(hex_)
        return bytes.fromhex(hex_)

    def _stat_cmd(self, stat_format=StatResult.stat_format, path=None):
        # TODO use _stat_format_darwin for cases where gstat is missing
        cmd = 'gstat' if self.remote_platform == 'darwin' else 'stat'
        if path is None:
            path = self.rpath

        if path == '':
            _path = path
        else:
            _path = f' "{path}"'

        return f'{cmd} -c {stat_format}{_path}'

    def stat(self):
        remote_cmd = self._stat_cmd()
        out = self._ssh(remote_cmd)
        try:
            return StatResult(out)
        except ValueError as e:
            if out.endswith(b'Permission denied'):
                raise PermissionError(out.decode())

            elif out.endswith(b'No such file or directory'):
                raise FileNotFoundError(out.decode())

            else:
                log.error(remote_cmd)
                raise ValueError(out) from e

    def exists(self):
        try:
            st = self.stat()
            return bool(st)  # FIXME
        except FileNotFoundError:  # FIXME there will be more types here ...
            pass

    @property
    def __parent(self):  # no longer needed since we inherit from path directly
        # because the identifiers are paths if we move
        # file.ext to another folder, we treat it as if it were another file
        # at least for this SshRemote path, if we move a file on our end
        # the we had best update our cache
        # if someone else moves the file on the remote, well, then
        # that file simply vanishes since we weren't notified about it
        # if there is a remote transaction log we can replay if there isn't
        # we have to assume the file was deleted or check all the names and
        # hashes of new files to see if it has moved (and not been changed)
        # a move and change without a sync will be bad for us

        # If you have an unanchored path then resolve()
        # always operates under the assumption that the
        # current working directory which I think is incorrect
        # as soon as you start passing unresolved paths around
        # the remote system doesn't know what context you are in
        # so we need to fail loudly
        # basically force people to manually resolve their paths
        return self.__class__(self.cache.parent)  # FIXME not right ...

    def is_dir(self):
        remote_cmd = self._stat_cmd(stat_format="%F")
        out = self._ssh(remote_cmd)
        return out == b'directory'

    def is_file(self):
        remote_cmd = self._stat_cmd(stat_format="%F")
        out = self._ssh(remote_cmd)
        return out == b'regular file'

    @property
    def children(self):
        # this is amusingly bad, also children_recursive ... drop the maxdepth
        #("find ~/files/blackfynn_local/SPARC\ Consortium -maxdepth 1 "
        #"-exec stat -c \"'%n' %o %s %W %X %Y %Z %g %u %f\" {} \;")
        # chechsums when listing children? maybe ...
        #\"'%n' %o %s %W %X %Y %Z %g %u %f\"
        if self.is_dir():
            # no children if it is a file sadly
            remote_cmd = (f"cd {self.rpath};"
                          f"{self._stat_cmd(path='')} {{.,}}*;"
                          "echo '----';"
                          f"{self.cypher_command} {{.,}}*;"  # FIXME fails on directories destroying alignment
                          'cd "${OLDPWD}"')

            out = self._ssh(remote_cmd)
            stats, checks = out.split(b'\r\n----\r\n')
            #print(stats)
            stats = {sr.name:sr for s in stats.split(b'\r\n')
                     for sr in (StatResult(s),)}
            checks = {fn:bytes.fromhex(cs) for l in checks.split(b'\r\n')
                      if not b'Is a directory' in l
                      for cs, fn in (l.decode(self.encoding).split('  ', 1),)}

            return stats, checks  # TODO

    def _mkdir_child(self, child_name):
        raise NotImplementedError('implement in subclass and/or fix instantiation/existence issues')

    def __repr__(self):
        return f'{self.__class__.__name__}({self.rpath!r}, host={self.host!r})'


SshRemote._bind_flavours()
