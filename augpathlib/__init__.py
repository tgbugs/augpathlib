from augpathlib.meta import PathMeta
from augpathlib.core import (AugmentedPath,
                             AugmentedPathPosix,
                             AugmentedPathWindows,
                             XopenPath,
                             LocalPath,
                             EatPath)
from augpathlib.caches import (CachePath,
                               PrimaryCache,
                               SqliteCache,
                               SymlinkCache,
                               EatCache,
                               SshCache)
from augpathlib.remotes import RemotePath
from augpathlib.utils import StatResult, FileSize, etag
from augpathlib.zip import ZipHelper, ZipPath

try:
    from augpathlib.repo import RepoHelper, RepoPath
except ImportError as e:
    class RepoHelper:
        def __init__(self, *args, __error=e, **kwargs):
            raise ImportError(f'{self.__class__.__name__} could not be imported '
                              'due to a previous ImportError') from __error


    class RepoPath(RepoHelper):
        pass


__all__ = [
    'StatResult',
    'FileSize',
    'etag',

    'PathMeta',

    'AugmentedPath',
    'XattrPath',
    'RepoPath',
    'XopenPath',
    'LocalPath',

    'CachePath',
    'PrimaryCache',
    'SqliteCache',
    'SymlinkCache',
    'XattrCache',
    'SshCache',

    'RemotePath',
]

__version__ = '0.0.25'
