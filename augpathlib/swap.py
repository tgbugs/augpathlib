import os
import sys
import ctypes

# linux renameat2
# osx renamex_np
# osx exchangedata
# win MoveFileTransacted very few systems support this

def swap_not_implemented(self, target):
    raise NotImplementedError('This OS has no atomic swap operation!')


if sys.platform == 'linux':  # windows will error on ctypes.CDLL without this
    # setup for calling renameat2
    SYS_renameat2 = 316  # from /usr/include/asm/unistd_64.h
    RENAME_EXCHANGE = (1 << 1)  # /usr/src/linux/include/uapi/linux/fs.h
    libc = ctypes.CDLL(None)
    rnat2_syscall = libc.syscall
    rnat2_syscall.restypes = ctypes.c_int       # returns an int
    rnat2_syscall.argtypes = (ctypes.c_int,     # syscall number
                              ctypes.c_int,     # old dir fd
                              ctypes.c_char_p,  # oldpath
                              ctypes.c_int,     # new dir fd
                              ctypes.c_char_p,  # newpath
                              ctypes.c_uint)    # flags

    def swap_linux(self, target):
        """ use renameat2 to perform an atomic swap operation """
        old_fd = os.open(self, 0)
        new_fd = os.open(target, 0)
        old_path = os.fspath(self).encode()
        new_path = os.fspath(target).encode()
        value = rnat2_syscall(SYS_renameat2,
                              old_fd, old_path,
                              new_fd, new_path,
                              RENAME_EXCHANGE)
        os.close(old_fd)
        os.close(new_fd)
        if value != 0:
            raise OSError(value, 'renameat2 failed', self, None, target)
