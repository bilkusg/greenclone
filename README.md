greenclone
==========

copy windows hierarchies handling hard and soft links, perms, afs, VSS shadows and long filenames

Copyright (C) 2013 Gary M. Bilkus
Initial version September 2013


This project - greenclone - provides Windows users with an alternative to robocopy and other command line tools for 
making copies of directory hierarchies in a form which is browsable and can be used for an exact restore.

Its key features which distinguish it from other command line tools are:

- Preserves symbolic links and junction points

- Restores hardlinks as hardlinks, so if your hierarchy contains two or more hardlinks to the same file, the destination will be the same

- Can optionally store permissions and AFS information in an auxiliary file, so backups don't lose information even if the
destination is not NTFS ( useful for Linux NAS backups )

- Can optionally use VSS to make shadow copies

- Can use existing shadow copies or other arbitrary low-level windows paths ( things starting \\?\ in other words )


If you want detailed help in installing the binary package, please read INSTALL.txt
If you want help in compiling the program or understanding the source code, please read BUILD.txt


Usage: 
Note: to use greenclone propery you must be an administrator and run as administrator.

greenclone /? 
     will print a usage message

greenclone fromdir todir
     will copy the contents of fromdir to todir recursively. They should be specified in full with drive letter and full path using \ not /. The EXAMPLES.txt file contains detailed examples of what is and isn't valid.
     fromdir must exist and be readable
     todir need not exist and will be created if necessary

By default, greenclone will:
   - copy files including any alternate filestreams permissions or attributes
   - leave unchanged anything already on the destination 
   - skip files it can't open on the source because they are locked in use
   - notice if the target already exists and has the same size and exact modification time as the source, and if so skip it
In other words, by default, greenclone will not destroy any data already on the destination.

The following options are available
   /NP - do not copy permissions and attributes
   /W - overwrite files on the destination which are different, and delete any which are not on the source. /W is intended to have exactly the same effect as deleting the destination first, but without the overhead of copying files already the same.
   /K - store permissions, AFS and attributes in an auxiliary file ...bkfd which will be used to restore these if it is found during a copy back
   /SHADOW - create a VSS shadow copy of the source drive first
   /M - don't copy regular files, only reparse points  and the directory hierarchy. Useful for a quick fix for what robocopy leaves out
   /H - if source and destination are on the same filesystem, copy files by hardlinking from source to dest. Useful for making a space-efficient capture of what's already there, just before clobbering it.
   /M - only copy reparse points and the directory structure
   /V - verbose - report progress of files during transfer
   /Q - quiet - only report errors

As a simple example which will probably cover most usage:
For making backups from an in-use filesystem to a remote possibly non-windows share, do:

greenclone  C:\ \\Server\Share\Dir /SHADOW /W /K

To restore the above, do:
greenclone \\Server\Share\Dir C:\ /W


NOTES
If greenclone needs to change a file, it always deletes the original and creates a new one. This ensures that if the file were hardlinked elsewhere outside the hierarchy, the external hardlink will be preserved with its original contents and the  hardlink will be broken.

Although greenclone does its best to be reliable, there are some reasons it may fail. These include:
- not running as an adminstrator
- not having sufficient permissions on a remote share
- needing to delete files or directories on the destination which are locked/in use


Greenclone's /SHADOW option is fairly naive. It will work fine for backups of most home workstations, but will almost certainly not do everything it needs to for a complete backup of a complex server.  If the built-in shadow copy service doesn't meet your needs, you can always use other tools to create a shadow copy, and use the resulting path as source - greenclone is perfectly happy with \\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy23\ as the start of its path.
Please DON'T attempt to use /SHADOW on an existing shadow copy! 

greenclone doesn't have as many command line options as other tools. This is deliberate as it is intended to do one very important job and do it well. That said, the source code is freely available, and the main class, written in C#, provides somewhat more fine-grained control, and can easily be adapted for more specialised purposes.
