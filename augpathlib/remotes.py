from pathlib import PurePosixPath
from augpathlib import exceptions as exc
from augpathlib.meta import PathMeta
from augpathlib import caches


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
        if not isinstance(cache, caches.CachePath):
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


