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
mount point. This is done by calling the dorkfs binary with the "mount"
subcommand. This command takes three main arguments: A mount point, a cache
directory that contains cached data and intermediate files like the changed
files of the workspace, and a URL that points to the git repository that shall
be mounted to the mount point. An example call could look like this:

```
$ dorkfs mount /var/run/dorkcache/dorkfs /mnt/dorkfs \
github+https://github.com/darkwisebear/dorkfs 
```

See below for a more complete example.

Managing the workspace
----------------------

DorkFS sets up a special directory called .dork below the given mount point.
Inside this directory there is a link to a socket the daemon process listens 
to. The socket accepts incoming JSON objects and responds with an UTF-8 string
that is a status most of the time but it can also be a lot more data, for
example in case of the log command.

The data format of the JSON object is subject to change. However, it is kept
backwards compatible, so older (minor) versions of dorkfs may talk to newer
versions of a daemon process.

Using this interface is done by calling dorkfs with any command except mount.
Please check the command line help for a list of available commands.

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
$ dorkfs commit -m "This is a VCS hello world example"
$ dorkfs log | less
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
