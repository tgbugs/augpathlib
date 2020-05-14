import zipfile
from pathlib import PurePath
from augpathlib import AugmentedPath
from augpathlib import exceptions as exc
from augpathlib.utils import log as _log


class ZipInternalPath(AugmentedPath):  # should be a pure path, but need bind_flavours

    zip_file = None  # set in _new

    # _paths is basically the fake file system for the contents of the zip file
    _paths = tuple()  # set in _new, and should be a dict +set+ so new files can be added to a zip

    @classmethod
    def _new(cls, zip_path):
        zipath = type('ZipInternalPath',
                    (ZipInternalPath,),
                    dict(zip_file=zipfile.ZipFile(zip_path)))
        zipath._bind_flavours()
        paths = set(zipath(info=zi) for zi in zipath.zip_file.filelist)
        parents = set(parent for path in paths for parent in path.parents)
        zipath._paths = {p:p for p in parents | paths}
        return zipath

    def __new__(cls, *args, info=None, **kwargs):
        if not args and info is not None:
            args = info.filename,

        self = super().__new__(cls, *args, **kwargs)
        if self in cls._paths:
            return cls._paths[self]  # keep only a single copy

        if info is not None:
            self._zi = info

        return self

    @property
    def rchildren(self):
        # FIXME only the actual children not just the whole list
        for path in self._paths:
            if self in path.parents:  # FIXME horrible implementation
                yield path

    def exists(self):
        return self in self._paths

    def is_dir(self):
        return not hasattr(self, '_zi') or self._zi.is_dir()

    def __fspath__(self):
        if self.parent:
            raise TypeError('you need to pass opener= explicitly because python\'s '
                            'builtin open only works on actual files (sigh)')
        else:
            return super().__fspath__()

    def open(self, *args, **kwargs):
        if not self.is_dir():
            self.__fd = self.zip_file.open(self._zi, *args, **kwargs)
            # FIXME probably need a wrapped filed descriptor to clean up the
            # internal state of this class too?
            return self.__fd
        else:
            raise IsADirectoryError(self)

    def close(self):
        # FIXME very bad
        try:
            self.__fd.close()
        except AttributeError:
            raise BaseException('TODO')  # can't close a closed file?


ZipInternalPath._bind_flavours()


class ZipHelper:

    _zfc_cache = {}

    def exists(self):
        return (self.zip_path is None and super().exists() or
                self.zip_relative_path.exists())

    @property
    def ZipInternalPath(self):
        zp = self.zip_path
        if zp is not None and zp not in self._zfc_cache:
            zipath = ZipInternalPath._new(zp)
            self._zfc_cache[zp] = zipath

        return self._zfc_cache.get(zp, None)

    @property
    def zip_relative_path(self):
        if not hasattr(self, '_c_zip_relative_path'):
            self._c_zip_relative_path = None
            zp = self.zip_path
            if zp is not None:
                self._c_zip_relative_path = self.ZipInternalPath(self.relative_to(zp))

        # TODO consider whether we error on not in zip?
        return self._c_zip_relative_path

    path_relative_zip = zip_relative_path

    def asInternal(self):
        return self.zip_relative_path

    @property
    def zip_path(self):
        # like RepoPath.working_dir
        if not hasattr(self, '_c_zip_path'):
            parent = self.parent
            if parent == self:
                self._c_zip_path = None
            else:
                pzp = parent.zip_path  # zip files inside of zip files :/
                if pzp is None and self.suffix == '.zip' or zipfile.is_zipfile(self):
                    self._c_zip_path = self
                    self.ZipInternalPath()
                else:
                    self._c_zip_path = pzp

        return self._c_zip_path

    @property
    def rchildren(self):
        # FIXME internal to external path mangling
        if self.zip_path is not None:
            for child in self.zip_relative_path.rchildren:
                yield self.zip_path / child
        else:
            yield from super().rchildren

    def open(self, *args, **kwargs):
        if self.zip_path is not None:
            return self.path_relative_zip.open(*args, **kwargs)
        else:
            return super().open(*args, **kwargs)
            

class ZipPath(ZipHelper, AugmentedPath):
    pass


ZipPath._bind_flavours()
