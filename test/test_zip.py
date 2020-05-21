import os
import pathlib
import unittest
import pytest
import augpathlib as aug
from augpathlib import RepoPath, LocalPath, exceptions as exc
from .common import onerror, skipif_no_net, temp_path, this_file


def test():
    _zp =  this_file.parent / 'test.zip'
    # TODO create the zip we will test using mkdir, touch, and data = etc.
    zp = aug.ZipPath(_zp)

    zrp = zp.path_relative_zip
    # FIXME the internal paths should
    # be hidden as an implementation detail

    rc = sorted(zrp.rchildren)
    assert all([c.exists() for c in rc])
    grid = [(c.exists(), c.is_dir(), c)
            for c in sorted(zp.rchildren)]
    c = rc[-1]

    # TODO open_stream ...
    # to get actual generic behavior on open

    with c.open() as f:
        test1 = f.read()

    czp = zp / c
    with czp.open() as f:
        test2 = f.read()

    assert test1 == test2


if __name__ == '__main__':
    test()
