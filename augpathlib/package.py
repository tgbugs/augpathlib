import importlib.util
from urllib.parse import urlparse

import setuptools
from setuptools.dist import Distribution
from setuptools.command.egg_info import manifest_maker, FileList, log as eilog
from packaging.version import parse as parse_version

from .repo import RepoPath
from .utils import log as _log

log = _log.getChild('package')

eilog.set_threshold(99)

_orig_setup = setuptools.setup


def _run_setup(setup_file, *args, **kwargs):
    try:
        last_output = [None]
        def fake_setup(*args, _setup=setuptools.setup, **kwargs):
            last_output[0] = args, kwargs

        setuptools.setup = fake_setup
        spec = importlib.util.spec_from_file_location('setup', setup_file)
        setup = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(setup)
        args, kwargs = last_output[0]
        return setup, args, kwargs
    finally:
        setuptools.setup = _orig_setup


# https://docs.python.org/3/distutils/apiref.html#distutils.core.run_setup
setup_use_kwargs = False
if setup_use_kwargs:
    from distutils.core import run_setup as SIGH
    # yes the random writes to stdout are because we call run_setup
    # not just import it and yes, it was someone touching the root logger
    run_setup = _run_setup
else:
    from distutils.core import run_setup


def vinc(thing, prefix=None):
    if isinstance(thing, tuple):
        return (*thing[:-1], vinc(thing[-1]))
    elif isinstance(thing, str):
        raise TypeError("don't know how to increment a string")
    else:
        if thing is None:
            if prefix is not None:
                return prefix, 0
            else:
                return 0
        else:
            return thing + 1


def current_state(ver):
    if ver.local is not None: return 'local'
    if ver.post is not None: return 'post'
    if ver.pre is not None: return ver.pre[0]
    if ver.dev is not None: return 'dev'
    return 'release'


def logic(cstate, next_phase, rel_comp='release'):
    # if I want to go to major dev ? need modifier
    # TODO True -> toggle relese dev
    if next_phase == 'current': return cstate
    elif next_phase == 'dev':
        if cstate == 'dev': return cstate
        elif cstate in ('release', 'post', 'local'): return rel_comp, next_phase
        else: raise ValueError('cannot go to dev from a prerelease')
    elif next_phase == 'pre':  # this will bump a -> b -> rc since current will not
        if cstate == 'dev': return 'a'
        elif cstate == 'a': return 'b'
        elif cstate == 'b': return 'rc'
        elif cstate == 'rc': return 'rc'
        elif cstate in ('release', 'post', 'local'):
            return rel_comp, 'a'
        else: raise ValueError(f'wat c: {cstate} n: {next_phase}')
    elif next_phase in ('a', 'b', 'rc'):
        if cstate == 'dev': return next_phase
        elif cstate in ('a', 'b', 'rc') and cstate > next_phase:
            raise ValueError(f'cannot go back or skip a release c: {cstate} > n: {next_phase}')
        else: return rel_comp, next_phase
    elif next_phase == 'release':
        if cstate in ('dev', 'a', 'b', 'rc'): return None  # truncate
        else: return next_phase
    elif next_phase in ('major', 'minor', 'micro'): return next_phase
    elif next_phase == 'post':
        if cstate == 'release': return next_phase
        else: raise ValueError(f'can only post from release not from {cstate}')
    elif next_phase == 'local': return next_phase
    else: raise ValueError(f'wat c: {cstate} n: {next_phase}')


def cons_next(d, ver, next):
    # mutates in place
    if next in ('a', 'b', 'rc'):
        vp = ver.pre
        vn = vinc(vp[-1] if isinstance(vp, tuple) else vp)
        d.update(dict(pre=(next, vn)))
    elif next in ('dev', 'post'):
        d[next] = next, vinc(getattr(ver, next))
    elif next in ('release', 'major', 'minor', 'micro'):
        # FIXME this incorrect?
        release = d['release']
        if next == 'release':
            release = (*release[:-1], vinc(release[-1]))
        # FIXME index error or extent shorter version to that?
        elif next == 'major':
            release = vinc(release[0]), *[0 for _ in release[1:]]
        elif next == 'minor':
            release = (*release[:1], vinc(release[1]), *[0 for _ in release[2:]])
        elif next == 'micro':
            release = (*release[:2], vinc(release[2]), *[0 for _ in release[3:]])
        else: raise ValueError('hmr?')

        d['release'] = tuple(release)
    elif next == 'local':
        d.update(ver._version._asdict())
        d['local'] = vinc(ver.local),
    elif next is None:
        # FIXME this fails if you want to bump the release to go from release to dev
        # e.g. 0.1.27 -> 0.1.27.dev0 which is not a valid transition
        pass  # truncate to release from dev and pre
    else:
        raise ValueError('wat')


def next_version(ver, next_phase='current', rel_comp='release'):
    cstate = current_state(ver)
    next = logic(cstate, next_phase, rel_comp)
    d = dict(epoch=ver.epoch,
             release=ver.release,
             dev=None,
             pre=None,
             post=None,
             local=None,)
    if isinstance(next, tuple):
        dowhatnow, next = next
        cons_next(d, ver, dowhatnow)
        cons_next(d, ver, next)
    else:
        cons_next(d, ver, next)

    _nver = ver._version._replace(**d)
    _newver = ver.__class__('0')
    _newver._version = _nver
    # have to stringify so _key updates so comparisons are valid
    # yay for leaking implementation details
    newver = ver.__class__(str(_newver))
    return newver


class PackagePath(RepoPath):
    # TODO get latest release info from github and pypi
    _debug = False
    _sk = setup_use_kwargs
    @property
    def setupfu(self):
        # FIXME sometimes this can fail if there are nested setup.py files !??!
        # and the base path is relative !??! temp workaround is to resolve all
        # paths before use, but there is still a bug
        import logging
        sigh = logging.getLogger().level
        with self.folder:
            #print('cwd', aug.AugmentedPath.cwd(), '\nsf', self.setup_file)
            try:
                return run_setup(self.setup_file, stop_after='config')
            finally:
                logging.getLogger().setLevel(sigh)

    @property
    def setup_dist(self):
        if not hasattr(self, '_setup_dist'):
            duo = self.setupfu
            self._setup_dist = duo

        return self._setup_dist

    @property
    def setup_kwargs(self):
        #raise NotImplementedError('use setup_dist')
        if not hasattr(self, '_setup_kwargs'):
            mod, args, kwargs = self.setupfu
            self._setup_kwargs = kwargs

        return self._setup_kwargs

    def _bind_requests(self):
        if not hasattr(self, '_requests'):
            import requests
            self._requests = requests

    @property
    def pypi_json(self):
        if not hasattr(self, '_pypi_json'):
            self._bind_requests()
            self._pypi_request = self._requests.get(f'https://pypi.org/pypi/{self.arg_packagename}/json')
            if not self._pypi_request.ok:
                # new package with no existing releases
                return

            self._pypi_json = self._pypi_request.json()
            # XXX sometimes the pypi api spews stale data :/
            if self._debug:
                import pprint
                pprint.pprint(self._pypi_json)

        return self._pypi_json

    @property
    def github_json(self):
        if not hasattr(self, '_github_json'):
            self._bind_requests()
            self._github_request = self._requests.get(self.remote_uri_api('/releases'))
            if not self._github_request.ok:
                return  # e.g. hit rate limit when testing

            self._github_json = self._github_request.json()

        return self._github_json

    @property
    def version_latest_pypi(self):
        pj = self.pypi_json
        if pj:
            return parse_version(pj['info']['version'])
        #return Version(self.pypi_json['info']['version'])

    @property
    def version_latest_released(self):
        # git, pypi, tag??
        pj = self.pypi_json
        if pj:
            vers = sorted(parse_version(_) for _ in pj['releases'])
            #vers = sorted(Version(_) for _ in self.pypi_json['releases'])
            return vers[-1]

    @property
    def version_latest_github(self):
        lpn = len(self.arg_packagename) + 1 if self.tag_prefix else 0
        tlg = self.tag_latest_github
        if tlg:
            version = tlg[lpn:]
            return parse_version(version)

    @property
    def tag_latest_github(self):
        gj = self.github_json
        if gj:
            if self.tag_prefix:
                these = [r for r in gj if self.arg_packagename in r['tag_name']]
            else:
                these = [r for r in gj if r['tag_name'][0] in '0123456789']

            if these:
                latest = these[0]
                return latest['tag_name']

    def version_next(self, next_phase='current', rel_comp='release'):
        # FIXME
        vlp = self.version_latest_pypi
        vlr = self.version_latest_released
        assert vlp == vlr, f'wat {vlp} != {vlr}'
        if vlp is not None:
            return next_version(vlp, next_phase=next_phase, rel_comp=rel_comp)
        elif self.version_repo:
            return self.version_repo
        else:
            return parse_version('0.0.0.dev0')  # FIXME hardcoded default zeroth version

    @property
    def tag_prefix(self):
        # TODO tag_prefix_anyway
        tag_prefix = False  # if for some reason we want to regularize version tagging that can go in the repo
        return self.setup_file.parent != self.working_dir or tag_prefix

    @property
    def tag(self):
        # the logic is that if module folder name == package name or we override via tag no rename
        # then there is no prefix expected, otherwise the prefix is ALWAYS the package name

        # FIXME there is no good way to do this without having it specified somewhere in
        # the repo that some package has priority for prefixless versions
        # also if someone renames the outer folder, which is entirely allowed and possible
        # then the tag will change, however I think I can do better because the logic is
        # actually about whether setup.py is in the root of the repo NOT whether names
        # match ... HRM

        if self.tag_prefix:
            match_version = self.arg_packagename + '-*'
        else:
            match_version = '[0-9]*'

        try:
            return self.repo.git.describe('--abbrev=0', '--tags', f'--match={match_version}')
        except self._git.exc.GitCommandError as e:
            pass  # no tag for this version

    @property
    def version_tag(self):
        lpn = len(self.arg_packagename) + 1 if self.tag_prefix else 0
        if self.tag:
            version = self.tag[lpn:]
            return parse_version(version)

    @property
    def version_repo(self):
        if self._sk:
            return parse_version(self.setup_kwargs['version'])
        else:
            return parse_version(self.setup_dist.get_version())
        #return Version(self.setup_kwargs['version'])

    @property
    def _version_new(self):  # XXX unused
        # TODO cases dev normal
        # want dev release but repo is at an unreleased normal
        # want normal, already released this one
        # want dev, already released this one
        # want *, repo skips a version
        return self.version_repo
        raise NotImplementedError('TODO')

    @property
    def release_files(self):
        # use to get the list of files that will be included in a release
        # so that we can limit the log to only those files
        mm = manifest_maker(Distribution())
        mm.distribution.script_name = 'setup.py'  # FIXME check path on this one
        mm.manifest = 'MANIFEST.in'
        mm.filelist = FileList()
        with self.folder:
            mm.add_defaults()
            mm.read_template()
            mm.add_license_files()

        mm.prune_file_list()
        mm.filelist.files += ['MANIFEST.in']
        mm.filelist.sort()
        mm.filelist.remove_duplicates()
        return mm.filelist.files

    def commits_since_last_release(self):
        try:
            rfs = [(self.folder / f) for f in self.release_files]
        except FileNotFoundError as e:  # no MANIFEST.in usually
            print(e)
            rfs = [self.folder]

        _tag = self.tag
        tag = _tag if _tag else ''
        log = self.repo.git.log("--format='%aI %an %h %s'",
                                f'{tag}..HEAD',
                                '--', *rfs)
        entries = [e[1:-1] for e in log.split('\n')]
        return entries

    @property
    def module_init_file(self):
        return self.module / '__init__.py'

    @property
    def module(self):
        if self._sk:
            kwargs = self.setup_kwargs
            name = kwargs['name']
            packages = kwargs['packages']
        else:
            name = self.arg_packagename
            packages = self.setup_dist.packages

        for package in packages:
            if package == name:
                return self.folder / name

        raise NotImplementedError(f'Don\'t know how to release packages whose name does not match a package name. {name} {packages}')

    @property
    def setup_file(self):
        return self.folder / 'setup.py'

    @property
    def folder(self):
        if not self.is_absolute() or '..' in self.parts:
            return self.resolve().folder

        if self.is_dir():
            for f in self.glob('setup.py'):
                return self

        if self.parent == self:
            raise ValueError('No setup.py found.')

        return self.parent.folder

    @property
    def arg_org(self):
        u = urlparse(self.remote_uri_human())
        _, org, repo, *_ = u.path.split('/')
        return org

    @property
    def arg_repo(self):
        u = urlparse(self.remote_uri_human())
        _, org, repo, *_ = u.path.split('/')
        return repo

    @property
    def arg_folder(self):
        return self.folder.relative_to(self.working_dir.parent)

    @property
    def arg_packagename(self):
        if self._sk:
            return self.setup_kwargs['name']
        else:
            return self.setup_dist.get_name()

    @property
    def arg_rest(self):
        # TODO
        return ''

    def command(self, next_phase='current', rel_comp='release'):
        rest = self.arg_rest
        rest = ' ' + self.rest if rest else ''
        nv = self.version_next(next_phase, rel_comp)
        return (
            f'build-release {self.arg_org} {self.arg_repo} {self.arg_folder} '
            f'{self.arg_packagename} {nv}{rest}')

    def bump(self, next_phase='current', rel_comp='release', pretend=False):
        nv = self.version_next(next_phase=next_phase, rel_comp=rel_comp)
        if nv == self.version_latest_released:  # we should never hit this branch
            raise ValueError(f'already released {nv}')
        elif nv == self.version_repo:
            raise ValueError(f'already bumped to {nv} but not released (though maybe not committed?)')
        breakpoint()
        if pretend:
            print('would bump module', self.module_init_file,
                  'for package name', self.arg_packagename,
                  'from', self.version_repo,
                  'to', nv)
            return
        # make the change in __init__ (or wherever)
        # commit the change
        # do NOT PUSH the change

PackagePath._bind_flavours()
