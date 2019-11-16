# Multi-threaded copy tool

A tool to copy files in a multi-threaded fashion.  
Useful when copying large amount of small files to a disk mounted over network (NAS --- NAS, local <---> NAS)) for ex.  

Performance gain of up to 10X vs `rsync` when copying a with 250.000 files to a NAS mount, in a tree structure of 3 levels of folders ending each in one file.

This is just another implementation of the [threaded_parser](https://github.com/magorbalassy/threaded_parser) tool.

Use `create_files.sh` to create dummy test folder with files.