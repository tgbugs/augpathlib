

class AugPathlibError(Exception):
    """ base class for augpathlib errors """


class UnhandledTypeError(AugPathlibError):
    """ haven't dealt with this yet """


class PathExistsError(AugPathlibError):
    """ path exists so don't try to symlink """


class RemotePathExistsError(AugPathlibError):
    """ the remote path already exists so don't create it """


class DirectoryNotEmptyError(AugPathlibError):  # FIXME compare to PathNotEmpty
    """ directory is not empty """


class TargetPathExistsError(PathExistsError):
    """ when adding to a path if fail_on_exists is set raise this """


class PathNotEmptyError(AugPathlibError):  # FIXME iirc used in clone, compare with DirectoryNotEmpty
    """ folder has children and is not empty, don't overwrite """


class WillNotRemovePathError(AugPathlibError):
    """ we will not even try to remove this path unless you enter the DANGERZONE """


class MetadataIdMismatchError(AugPathlibError):
    """ there is already cached metadata and id does not match """


class MetadataCorruptionError(AugPathlibError):
    """ there is already cached metadata and id does not match """


class CircularSymlinkNameError(AugPathlibError):
    """ path name does not match name embedded in symlink """


class NoStreamError(AugPathlibError):
    """ no stream with that name exists similar to file not found or no data """


class NoFileIdError(AugPathlibError):
    """ no file_id """


class NoCachedMetadataError(AugPathlibError):
    """ there is no cached metadata """


class ChecksumError(AugPathlibError):
    """ utoh """


class SizeError(AugPathlibError):
    """ really utoh """


class CommandTooLongError(AugPathlibError):
    """ not the best solution ... """


class NoRemoteMappingError(AugPathlibError):
    """ prevent confusion between local path data and remote path data """


class NotInProjectError(AugPathlibError):
    """fatal: not a spc directory {}"""
    def __init__(self, message=None):
        if message is None:
            more = '(or any of the parent directories)' # TODO filesystem boundaries ?
            self.message = self.__doc__.format(more)


class BootstrappingError(AugPathlibError):
    """ Something went wrong during a bootstrap """


class NoSourcePathError(AugPathlibError):
    """ dictionary at some level is missing the expected key """


class CacheIgnoreError(AugPathlibError):
    """ Path is excluded via cache_ignore. """


class LocalChangesError(AugPathlibError):
    """ File has local changes """


class NoMetadataRetrievedError(AugPathlibError):
    """ we failed to retrieve metadata for a remote id """


class NoRemoteFileWithThatIdError(AugPathlibError):
    """ the file you are trying to reach has been disconnected """


class CacheNotFoundError(AugPathlibError):
    """ no local cache and no remote cache for file

        use this to provide the local path dual of a remote
        for better error reporting """


class WhyDidntThisGetMovedBeforeError(AugPathlibError):
    """ file should already have been moved ... """


class RepoExistsError(AugPathlibError):
    """ a repository already exists at this path """


class NotInRepoError(AugPathlibError):
    """ a repository does not exist in this
        or any of the parents of this path """


class NoCommitsForFile(AugPathlibError):
    """ could not find any commits for file """


class InvalidRefError(AugPathlibError):
    """ ref.is_valid() returned False """


class RemoteAlreadyAnchoredError(AugPathlibError):
    """ a Remote class has already been anchored locally """


class CacheExistsError(AugPathlibError):
    """ attempting to init a cache that already exists """


class FileHasNotChangedError(AugPathlibError):
    """ signal cases where a file has not changed
        and thus that no action will be taken """
