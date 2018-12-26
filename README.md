DorkFS
======

Purpose
-------

DorkFS is a versioned, FUSE based file system aiming at the management
of large repositories that consist of multiple sources, including Git
repositories, Artifactory stores and other backends.

Structure
---------

DorkFS is designed to lazily download needed artifacts as they are
accessed through the file system layer. It manages a local workspace
that accesses the versioned artifacts in a copy-on-write manner. On
commit, the changed objects are stored in the local cache and uploaded
to the remote storages.

State
-----

* DorkFS uses the local file system to create the workspace and the
  local cache.
* A GitHub driver exists so that it is possible to mount Git
  repositories hosted on github.com or an on-premises installation
  of GitHub.
* The workspace overlay is working so that you can create and modify the
  cached files.

Usage
=====

Mounting the file system
------------------------

DorkFS sets up a local storage and mounts its contents to the given
mount point. Currently, mounting supports the following options:
```
dorkfs

USAGE:
    dorkfs [OPTIONS] <cachedir> <mountpoint> <rootrepo>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --branch <branch>    Remote branch that shall be tracked instead
of the default branch
        --gid <gid>          GID to be used for the files. Defaults to
the effective GID of the process.
        --uid <uid>          UID to be used for the files. Defaults to
the effective UID of the process.
        --umask <umask>       [default: 022]

ARGS:
    <cachedir>      Directory where the cached contents shall be stored
    <mountpoint>    Mountpoint that shows the checked out contents
    <rootrepo>      Connection specification to the root repository. For
GitHub this string has the following form:
github+<GitHub API URL>/<org>/<repo>
```

Managing the workspace
----------------------

DorkFS sets up a special directory called .dork below the given mount
point. The files in this directory are used to interact with the file
system driver. Currently, this directory provides the following special
files:
* commit: This is a write-only file. Writing a descriptive message to
  this file commits the local changes and uses the message as the commit
  message of the newly created commit.
* log: This file is read-only. It provides the commit log of the
  currently mounted workspace.
* HEAD: This file shows the current base commit used to display
  contents. Echoing 'latest' to this file will update the repository to
  the latest commit of the tracked branch. If the workspace is unclean,
  the update is refused.
* current_branch: This file displays the currently tracked remote
  branch. Echoing a branch name to this file will switch the tracked
  branch. It will, however, _not_ change the current commit used as the
  base for the mounted fs. Use HEAD to update to the latest commit of
  the tracked branch.
* create_branch: This is a write-only file. Creates a branch with the
  written string as its name and sets it to the current commit HEAD
  points to.
* revert: This is a write-only file. Reverts the file or directory
  given. The string has to be a path relative to the mount point.
* status: This is a read-only file. Displays the status of the
  workspace. The first column shows the status of the file or directory:
  'D' denotes a deleted file or dir, 'A' denotes added files,
  and 'M' marks modified files. The second column shows the path to the
  file or directory as a relative path to the mount point.

Examples
========

Mounting a workspace and committing a file
------------------------------------------

The following commands

```
$ dorkfs --uid johndoe --gid johndoe /home/johndoe/dorkcache \
/mnt/dorkfs github+https://api.github.com/darkwisebear/dorktest
$ cd /mnt/dorkfs
$ mkdir testdir
$ cd testdir
$ echo "Hello, world!" > hello_world.txt
$ cd ../.dork
$ echo "This is a VCS hello world example" > commit
$ cat log
```

will give (example):

```
On branch (HEAD)
Commit: 8812da4fb607d7def1cc738b64b6eb63f941fb7fca16dc8ccb9c0d24987d08af

This is a VCS hello world example


Tree:   3b8556c149d27c98cf8bd4bdf4f7acaebabc529656109e764aa38ee1e5c78104
```

Known issues
============

* Currently, no signal handler for SIGTERM is installed. Therefore,
  unmounting should be done using umount as sending SIGTERM will not be
  handled properly.
