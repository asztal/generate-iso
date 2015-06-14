The project is written in C#. It does not yet implement the following features:
  * Volume spanning
  * Rock ridge
  * Joliet
  * Apple ISO-9660 extensions
  * Logical block sizes smaller than the logical sector size

However, it does implement:
  * Boot sectors (El Torito specification)
  * Primary and supplementary volumes
  * Level 1 and 2 ISO-9660 compatibility modes
  * Auto-generation of 8.3/truncated/uppercase filenames where source file names are invalid, and flags to disable it
  * Quite comprehensive error checking

There are still a couple of areas where the implementation isn't yet compliant:
  * The path table records and directory records are not yet sorted in the correct order as defined by ISO-9660