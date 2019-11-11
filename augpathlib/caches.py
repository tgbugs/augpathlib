import pathlib
from augpathlib import exceptions as exc
from augpathlib.meta import PathMeta
from augpathlib.core import AugmentedPath, EatHelper
from augpathlib.utils import log, LOCAL_DATA_DIR, default_cypher
from augpathlib import remotes


class CachePath(AugmentedPath):
    # CachePaths this needs to be a real path so that it can navigate the local path sturcture
    # FIXME Not sure I believe that, given the tradeoff
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

        # a nice side effect of weighing anchor here is that it
        # enforces order of operations for setup then init etc.
        if hasattr(cls, '_anchor'):
            cls.weighAnchor()

    @classmethod
    def weighAnchor(cls):
        # FIXME should this return the old anchor?
        acls = cls._abstract_class()
        if hasattr(acls, '_anchor'):
            delattr(acls, '_anchor')
        else:
            raise ValueError(f'{self.__class__} not anchored')

    def anchorClassHere(self):
        """ Use this to initialize the class level anchor from an instance. """
        if not hasattr(self.__class__, '_anchor'):
            self.__class__._abstract_class()._anchor = self
        else:
            raise ValueError(f'{self.__class__} already anchored to {self.__class__._anchor}')

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
                    pass  # we don't use self._local anymore too many issues

            elif isinstance(path, LocalPath):
                path._cache = self  # FIXME don't do this?

            elif isinstance(path, remotes.RemotePath):
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
                if hasattr(self, '_local'):
                    log.error('why does this code path still use self._local?')
                    delattr(self._local, '_cache')

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
        if isinstance(key, remotes.RemotePath):
            # FIXME not just names but relative paths???
            remote = key
            try:
                child = self._make_child(remote._parts_relative_to(self.remote, self.parent), remote)
            except AttributeError as e:
                raise exc.AugPathlibError('aaaaaaaaaaaaaaaaaaaaaa') from e

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
        if isinstance(remote, remotes.RemotePath):
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

        # FIXME remove?
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

        do_cast = not isinstance(target, self.__class__)
        if do_cast:
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

                elif do_cast:
                    # the target meta was just put there, if the ids match it should be ok
                    # however since arbitrary meta can be passed in, best to double check
                    file_is_different = target._meta_updater(self.meta)
                    if file_is_different:
                        log.critical('Something has gone wrong'
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
            safe_unlink = target.local.parent / f'.unlink-{target.name}'
            try:
                if target.is_broken_symlink():
                    target.rename(safe_unlink)

                self.rename(target)  # if target is_dir then this will fail, which is ok
            except BaseException as e:
                log.exception(e)
                if safe_unlink.is_broken_symlink():
                    safe_unlink.rename(target)
            finally:
                if safe_unlink.is_broken_symlink():
                    safe_unlink.unlink()

        elif self.is_broken_symlink():
            # we don't move to trash here because this was just a file rename
            self.unlink()  # don't move the meta since it will break the naming insurance measure

        return target

    def __repr__(self):
        local = repr(self.local) if self.local else 'No local??' + str(self)
        remote = (f'{self.remote.__class__.__name__}({self.id!r})'
                  if self.remote else str(self.id))
        return self.__class__.__name__ + ' <' + local + ' -> ' + remote + '>'


CachePath._bind_flavours()


class ReflectiveCache(CachePath):
    """ Oh, it's me. """

    @property
    def meta(self):
        return self.local.meta


ReflectiveCache._bind_flavours()


class EatCache(EatHelper, CachePath):
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


EatCache._bind_flavours()


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


SqliteCache._bind_flavours()


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
                #assert pathlib.PurePosixPath(self.name) == self.readlink().parent.parent
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
                    if meta.updated is None and pathmeta.updated is None:
                        log.warning ('no change since either has an updated value (wat)')
                        return #FIXME

                    if meta.updated > pathmeta.updated:
                        log.info(msg.format(meta_newer))
                        return  # this is the right thing to do for a sane filesystem
                    elif meta.updated < pathmeta.updated:
                        log.info(msg.format(pathmeta_newer))
                        # THIS IS EXPLICITLY ALLOWED
                    else:  # they are equal
                        extra = 'Both updated at the same time '
                        if meta.created is not None and pathmeta.created is not None:
                            if meta.created > pathmeta.created:
                                log.info(msg.format(extra + meta_newer))
                                return
                            elif meta.created < pathmeta.created:
                                log.info(msg.format(extra + pathmeta_newer))
                                # THIS IS EXPLICITLY ALLOWED
                            else:  # same created
                                log.info(msg.format('Identical timestamps. Not updating.'))
                                return
                        elif meta.created is not None:
                            log.info(msg.format(extra + 'Meta has datetime other does not. Not updating.'))
                            return
                        elif pathmeta.created is not None:
                            msg = msg.format(extra + 'Meta has no datetime other does.')
                            log.info(msg)
                            raise exc.MetadataIdMismatchError(msg)
                        else:  # both none
                            log.info(msg.format(extra + ('Identical update time both missing created time. '
                                                         'Not updating.')))
                            return
                    # equality
                # id mismatch all cases above should return or raise except for other metadata newer

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
            #symlink = pathlib.PurePosixPath(self.local.name, pathmeta.as_symlink().as_posix().strip('/'))
            symlink = pathlib.PurePosixPath(self.local.name) / pathmeta.as_symlink()
            self.local.symlink_to(symlink)

        else:
            raise exc.PathExistsError(f'Path exists {self}')


SymlinkCache._bind_flavours()


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


PrimaryCache._bind_flavours()


class SshCache(PrimaryCache, EatCache):
    xattr_prefix = 'ssh'
    _backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache
    cypher = default_cypher

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


SshCache._bind_flavours()


# assign defaults

from augpathlib.core import LocalPath
SshCache._local_class = LocalPath
