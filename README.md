# augpathlib
[![PyPI version](https://badge.fury.io/py/augpathlib.svg)](https://pypi.org/project/augpathlib/)
[![Build Status](https://travis-ci.com/tgbugs/augpathlib.svg?branch=master)](https://travis-ci.com/tgbugs/augpathlib)
[![Coverage Status](https://coveralls.io/repos/github/tgbugs/augpathlib/badge.svg?branch=master)](https://coveralls.io/github/tgbugs/augpathlib?branch=master)

Augmented pathlib. Everything else you could do with a path.

## Introduction
Do you like pathlib?  
Have you ever wanted to see just how far you can push the path abstraction?  
Do you like using the division operator in ways that could potentially cause
reading from the network or writing to disk?  
Then augpathlib is for you!

## Details
augpathlib makes extensive use of the pathlib Path object (and friends)
by augmenting the base PosixPath object with additional functionality
such as getting and setting xattrs, syncing with other mapped paths etc.

In essence there are 3 ways that a Path object can be used: Local, Cache, and Remote.
Local paths return data and metadata that are local the the current computer.
Cache paths return local metadata about remote objects (such as their remote id).
Remote objects provide an interface to remote data that is associated with a path.

Remote paths should be back by another object which is the representation of the
remote according to the remote's APIs.

Remote paths are only intended to provide a 1:1 mapping, so list(local.data) == list(remote.data)
should always be true if everything is in sync.

If there is additional metadata that is associated with a local path then that is
represented in the layer above this one (currently DatasetData, in the future a validation Stage).
That said, it does seem like we need a more formal place that can map between all these
things rather than always trying to derive the mappings from data embedded (bound) to
the derefereced path object. 

## Installing on MacOS catalina 10.15.5+
If you are getting a failed build for pxattr, please continue reading the following.
When using LLVM (9.0.1+) or another 3rd party library for Clang on MacOS, there was an issue that requires a possible reinstall if you getting a failed build for pxattr. See [LLVM Issue](https://github.com/iustin/pyxattr/issues/25) for details. To reinstall LLVM on brew or anaconda you can use the following examples.
```bash
> brew reinstall llvm 
```
or 
```bash
> conda install --force-reinstall llvm 
```
If the reinstall fails to clear the issue consider uninstalling that library and use the default Clang from Xcode. It should be the following version or higher.
```bash
> clang -v
Apple clang version 11.0.3 (clang-1103.0.32.62)
Target: x86_64-apple-darwin19.5.0
Thread model: posix
InstalledDir: /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin
```
