DorkFS
======

Purpose
-------

DorkFS is a versioned, FUSE based file system aiming at the management of large repositories that consist of multiple
sources, incuding Git repositories, Artifactory stores and other backends.

Structure
---------

DorkFS is designed to lazily download needed artifacts as they are accessed through the file system layer. It manages a
local workspace that accesses the versioned artifacts in a copy-on-write manner. On commit, the changed objects are
stored in a local cache.

State
-----

* DorkFS uses the local file system to create the workspace and the local cache.
* The local cache is working so that you can commit changes and check the commit log.
* The workspace overlay is working so that you can create and modify the cached files.

Usage
=====

Mounting the file system
------------------------

DorkFS sets up a local storage and mounts its contents to the given mount point. Currently, mounting supports the
following options:
```
dorkfs.exe [OPTIONS] <cachedir> <mountpoint>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --gid <gid>         [default: 0]
        --uid <uid>         [default: 0]
        --umask <umask>     [default: 022]

ARGS:
    <cachedir>      Directory where the cached contents shall be stored
    <mountpoint>    Mountpoint that shows the checked out contents
```

Managing the workspace
----------------------

DorkFS sets up a special directory called .dork below the given mount point. The files in this directory are used to
interact with the file system driver. Currently, this directory provides the following special files:
* commit: This is a write-only file. Writing a descriptive message to this file commits the local changes and uses the
  message as the commit message of the newly created commit.
* log: This file is read-only. It provides the commit log of the currently mounted workspace.

Examples
========

Mounting a workspace and committing a file
------------------------------------------

The following commands

```
dorkfs --uid johndoe --gid johndoe /home/johndoe/dorkcache /mnt/dorkfs
cd /mnt/dorkfs
mkdir testdir
cd testdir
echo "Hello, world!" > hello_world.txt
cd ../.dork
echo "This is a VCS hello world example" > commit
cat log
```

will give:

```
On branch (HEAD)
Commit: 8812da4fb607d7def1cc738b64b6eb63f941fb7fca16dc8ccb9c0d24987d08af

This is a VCS hello world example


Tree:   3b8556c149d27c98cf8bd4bdf4f7acaebabc529656109e764aa38ee1e5c78104
```

Known issues
============

* Currently, no signal handler for SIGTERM is installed. Therefore, unmounting should be done using umount as sending
  SIGTERM will not be handled properly.