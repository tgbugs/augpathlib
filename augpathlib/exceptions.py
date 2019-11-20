

class AugPathlibError(Exception):
    """ base class for augpathlib errors """


class UnhandledTypeError(AugPathlibError):
    """ haven't dealt with this yet """


class PathExistsError(AugPathlibError):
    """ path exists so don't try to symlink """


class DirectoryNotEmptyError(AugPathlibError):
    """ directory is not empty """


class TargetPathExistsError(PathExistsError):
    """ when adding to a path if fail_on_exists is set raise this """


class PathNotEmptyError(AugPathlibError):
    """ folder has children and is not empty, don't overwrite """


class WillNotRemovePathError(AugPathlibError):
    """ we will not even try to remove this path unless you enter the DANGERZONE """


class MetadataIdMismatchError(AugPathlibError):
    """ there is already cached metadata and id does not match """


class MetadataCorruptionError(AugPathlibError):
    """ there is already cached metadata and id does not match """


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


class WhyDidntThisGetMovedBeforeError(AugPathlibError):
    """ file should already have been moved ... """


class RepoExistsError(AugPathlibError):
    """ a repository already exists at this path """


class NotInRepoError(AugPathlibError):
    """ a repository does not exist in this
        or any of the parents of this path """


class NoCommitsForFile(AugPathlibError):
    """ could not find any commits for file """
