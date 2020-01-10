from augpathlib.meta import PathMeta
from augpathlib.core import (AugmentedPath,
                             AugmentedPathPosix,
                             AugmentedPathWindows,
                             XopenPath,
                             LocalPath,
                             EatHelper)
from augpathlib.caches import (CachePath,
                               PrimaryCache,
                               SqliteCache,
                               SymlinkCache,
                               EatCache,
                               SshCache)
from augpathlib.remotes import RemotePath
from augpathlib.utils import StatResult, FileSize, etag

__all__ = [
    'StatResult',
    'FileSize',
    'etag',

    'PathMeta',

    'AugmentedPath',
    'XattrPath',
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

try:
    from autpathlib.repo import RepoHelper RepoPath
    __all__ += 'RepoPath'
except ImportError:
    pass

__version__ = '0.0.5'
