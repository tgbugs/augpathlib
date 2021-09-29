import os
import io
import stat
import base64
import hashlib
import logging
from pathlib import Path


def makeSimpleLogger(name, level=logging.INFO):
    # TODO use extra ...
    logger = logging.getLogger(name)
    logger.setLevel(level)
    ch = logging.StreamHandler()  # FileHander goes to disk
    fmt = ('[%(asctime)s] - %(levelname)8s - '
           '%(name)14s - '
           '%(filename)16s:%(lineno)-4d - '
           '%(message)s')
    formatter = logging.Formatter(fmt)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


log = makeSimpleLogger('augpathlib')

LOCAL_DATA_DIR = '.operations'
SPARSE_MARKER = '.sparse'
AUG_XATTR_PREFIX = 'augpathlib'
default_cypher = hashlib.blake2b
cypher_command_lookup = {hashlib.sha256:'sha256sum',
                         hashlib.blake2b:'b2sum'}
red = '\x1b[31m{}\x1b[0m'  # use as red.format(value)


def onerror_windows_readwrite_remove(action, name, exc):
    """ helper for deleting readonly files on windows """
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)


def fs_safe_id(string):
    """ Make a string safe for use on file systems.
    NOTE this does NOT gurantee uniqueness!
    NOTE this only deals with bad printable chars not
    the full pathology that one might encounter. """
    # see https://stackoverflow.com/questions/1976007/
    evils = ' \n\t*|:\/<>"\\?'
    for evil in evils:
        string = string.replace(evil, '-')
    return string


def sysidpath(ignore_options=False, path_class=Path):
    """ get a unique identifier for the machine running this function """
    # in the event we have to make our own
    # this should not be passed in a as a parameter
    # since we need these definitions to be more or less static

    failover = path_class('/var/tmp/machine-id')  # /var/tmp is more persistent than /tmp/

    if hasattr(path_class, 'access'):
        accessf = lambda p: p.access(os.R_OK)
    else:
        # pypy3 3.6 still needs as_poxix here :/
        accessf = lambda p: os.access(p.as_posix(), os.R_OK)

    if not ignore_options:
        options = (
            path_class('/etc/machine-id'),
            failover,  # always read to see if we somehow managed to persist this
        )
        for option in options:
            if (option.exists() and
                accessf(option) and
                option.stat().st_size > 0):
                    return option

    uuid = uuid4()
    with open(failover, 'wt') as f:
        f.write(uuid.hex)

    return failover


def machine_guid(ignore_options=False, path_class=Path):
    key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                         "SOFTWARE\\Microsoft\\Cryptography", 0,
                         winreg.KEY_READ | winreg.KEY_WOW64_64KEY)
    guid, status = winreg.QueryValueEx(key, 'MachineGuid')
    return guid


if os.name == 'nt':
    import winreg
    def _raw_id(cls, cypher=default_cypher):
        if ((hasattr(cls, '_cache_class') and
             hasattr(cls._cache_class, 'cypher') and
             cls._cache_class.cypher != cypher)):  # FIXME this could be static ...
            cypher = cls._cache_class.cypher

        chunk = machine_guid().encode()
        m = cypher()
        m.update(chunk)
        return m.digest()

else:
    from uuid import uuid4
    def _raw_id(cls):
        return cls(sysidpath(path_class=cls)).checksum()[:16]


def _bind_sysid_(cls):
    if cls.sysid is None:
        cls.sysid = (base64
                     .urlsafe_b64encode(_raw_id(cls))[:-2]
                     .decode())
    else:
        raise ValueError(f'{cls} already has sysid {cls.sysid}')


class FileSize(int):

    @classmethod
    def ofPath(cls, path):
        """ return size of a path alt FileSize.of(path) ? """
        return cls(path.stat().st_size)

    @property
    def mb(self):
        return self / 1024 ** 2

    @property
    def hr(self):
        """ human readable file size """

        def sizeof_fmt(num, suffix=''):
            for unit in ['','K','M','G','T','P','E','Z']:
                if abs(num) < 1024.0:
                    return "%0.0f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Yi', suffix)

        if self is not None and self >= 0:
            return sizeof_fmt(self)
        else:
            return '??'  # sigh

    def __str__(self):
        return super().__repr__()

    def __repr__(self):
        return f'{self.__class__.__name__} <{self.hr} {self}>'


class StatResult:
    stat_format = f'\"%n  %i  %o  %s  %w  %W  %x  %X  %y  %Y  %z  %Z  %g  %u  %f\"'

    #stat_format = f'\"\'%n\' %o %s \'%w\' %W \'%x\' %X \'%y\' %Y \'%z\' %Z %g %u %f\"'

    _stat_format_darwin = '\"%N  %i  %k  %z  %SB  %B  %Sa  %a  %Sm  %m  %Sc  %c  %g  %u  %Xp\"'
    # also need -t '%F %T %z' for the %SB and friends, unfortunately it seems nanoseconds is missing?
    # so probably better to check for gstat first  # also -f instead of -c

    def __init__(self, out):
        out = out.decode()
        #name, rest = out.rsplit("'", 1)
        #self.name = name.strip("'")
        #print(out)
        wat = out.split('  ')
        #print(wat)
        #print(len(wat))
        name, ino, hint, size, hb, birth, ha, access, hm, modified, hc, changed, gid, uid, raw_mode = wat

        self.name = name

        def ns(hr):
            date, time, zone = hr.split(' ')
            time, ns = time.split('.')
            return '.' + ns

        self.st_ino = int(ino)
        self.st_blksize = int(hint)
        self.st_size = int(size)
        #self.st_birthtime
        self.st_atime = float(access + ns(ha)) 
        self.st_mtime = float(modified + ns(hm))
        self.st_ctime = float(changed + ns(hc))
        self.st_gid = int(gid)
        self.st_uid = int(uid)
        self.st_mode = int.from_bytes(bytes.fromhex(raw_mode), 'big')

        if hb != '-' and birth != '0':
            self.st_birthtime = float(birth + ns(hb))


class etag:

    cypher = hashlib.md5

    def __init__(self, chunksize):
        self.chunksize = chunksize
        self._m = self.cypher()
        self._parts = []
        self._remainder = b''
        self._last_chunksize = 0

    def update(self, bytes_):
        remainder, self._remainder = self._remainder, b''
        bio = io.BytesIO(remainder + bytes_)  # inefficient for len(bytes_) << self.chunksize
        while True:
            if self._last_chunksize:
                # in the event a restart is required only
                # only read the number of bytes left for
                # the current chunk to update self._m
                chunksize = self.chunksize - self._last_chunksize
                self._last_chunksize = 0
            else:
                chunksize = self.chunksize

            chunk = self._remainder
            self._remainder = bio.read(chunksize)
            if not self._remainder:
                # the second time around at the end this will break the loop if the chunk
                # is exactly equal to the chunk size
                if len(chunk) < chunksize:
                    self._remainder = chunk
                    return

            if chunk:
                self._m.update(chunk)  # empty remainder doesn't advance the function
                digest = self._m.digest()
                self._m = self.cypher()
                if chunksize < self.chunksize:
                    # we are in a restart and need to update the last part with the
                    # digest for the full chunksize
                    self._parts[-1] = digest
                else:
                    self._parts.append(digest)

    def digest(self):
        if self._remainder:
            self._last_chunksize = len(self._remainder)  # in case someone calls an early digest
            self._m.update(self._remainder)
            self._parts.append(self._m.digest())  # do not overwrite self._m yet
            self._remainder = b''  # this allows a stable call at the end

        m = self.cypher()
        m.update(b''.join(self._parts))
        return m.digest(), len(self._parts)

    def hexdigest(self):
        digest, count = self.digest()
        return f'{digest.hex()}-{count}'
