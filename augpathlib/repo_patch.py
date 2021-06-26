import git


class _Repo(git.Repo):  # FIXME should we subclass Repo for this or patch ??
    """ monkey patching """

    def getRef(self, ref_name):
        for ref in self.refs:
            if ref.name == ref_name:
                return ref

        else:
            raise ValueError(f'No ref with name: {ref_name}')

    def currentRefName(self):
        "return the branch, tag, or "
        if self.head.is_detached:
            hexsha, *ref_name = self.head.commit.name_rev.split()
            if ref_name:
                ref_name = ref_name[0]
                if '/' in ref_name:
                    tags, tag_name = ref_name.split('/', 1)
                    if tags == 'tags':
                        return tag_name
                    elif '~' in tag_name:
                        # these refs are unstable because they count
                        # backward from the first named ref that
                        # occures after them in the tree, better to
                        # use the hexsha in those cases
                        return None
                    else:
                        return ref_name
                else:
                    return None
            else:
                # yes hashes are technically refs, but we don't know
                # what the caller will want to do in that situation so
                # we return None so they can decide for themselves
                return None
        else:
            return self.active_branch.name


# monkey patch git.Repo
git.Repo.getRef = _Repo.getRef
git.Repo.currentRefName = _Repo.currentRefName


class _Reference(git.Reference):
    """ monkey patching """
    def __enter__(self):
        """ Checkout the ref for this head.
        `git stash --all` beforehand and restore during __exit__.

        If the ref is the same, then the stash step still happens.
        If you need to modify the uncommitted state of a repo this
        is not the tool you should use. """

        if not self.is_valid():
            raise exc.InvalidRefError(f'Not a valid ref: {self.name}')

        self.__original_branch = self.repo.active_branch
        self.__stash = self.repo.git.stash('--all')  # always stash
        if self.__stash == 'No local changes to save':
            self.__stash = None

        if self == self.__original_branch:
            return self

        self.checkout()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        _stash = self.repo.git.stash('--all')  # always stash on the way out as well
        if _stash == 'No local changes to save':
            stash = 'stash@{0}'
        else:
            stash = "stash@{1}"

        if self.__original_branch != self:
            self.__original_branch.checkout()

        # TODO check to make sure no other stashes were pushed on top
        if self.__stash is not None:
            self.repo.git.stash('pop', stash)

        self.__stash = None


# monkey patch git.Reference
git.Reference.__enter__ = _Reference.__enter__
git.Reference.__exit__ = _Reference.__exit__
