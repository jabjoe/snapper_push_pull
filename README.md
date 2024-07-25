What is this?
-------------

Simple tool to push and pull snapper snapshots between machines.

So basically, if you have a nice btrfs snapper setup, but want to sync those snapshots to a backup machine, this my solution.

It sends the snapshots incrementally and removes defunct snapshots.


Help
----

    usage: Snapper_push_pull [-h] [--dryrun] [-v] [-d] [-f] [-l]
                             [src_path] [dst_path]
    
    Tool to incrementally push or pull snapper snapshots between machines.
    
    positional arguments:
      src_path       snapper controlled source directory: /mnt/mylocal/.snapshots
                     or root@remote:/mnt/backup/.snapshots
      dst_path       destination directory: /mnt/mylocal/snapshots_backup or
                     root@remote:/mnt/backup/snapshots_backup
    
    options:
      -h, --help     show this help message and exit
      --dryrun       Don't acturally do it.
      -v, --verbose  Info log.
      -d, --debug    Debug log.
      -f, --force    Delete subvols at destination that conflict.
      -l, --list     Just list.
