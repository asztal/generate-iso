using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using IO = System.IO;

namespace Bomag.Build.Iso9660 {
    #region Enumerations
    public enum CompatibilityLevel {
        Level1,
        Level2,
        Level3
    }

    [Flags]
    public enum CompatibilityFlags {
        None = 0,
        
        /// <summary>
        /// Limits the depth of the directory hierarchy to that specified in the ISO-9660 standard (8).
        /// </summary>
        LimitDirectories = 1,
        
        /// <summary>
        /// Allows the implementation to truncate file names to either 8.3, 30 characters, or 31 characters depending on the
        /// compatibility level. This does not, however, protect against exceeding the 255 character path limit.
        /// </summary>
        TruncateFileNames = 2,

        /// <summary>
        /// Allows the implementation of convert file names to upper case when writing disk images in Compatibility Level 1.
        /// Without this flag, any lower case characters will be stripped from file names.
        /// </summary>
        UpperCaseFileNames = 4,
        
        /// <summary>
        /// Allows the implementation to try to resolve name conflicts by using tilde file names (e.g. MICROSOF, MICROS~1, MICROS~2)
        /// to distinguish between file names that would otherwise be identical. 
        /// </summary>
        ResolveNameConflicts = 8,
        
        /// <summary>
        /// File names with multiple dots are illegal in all compatibility levels - strip extra dots from the name.
        /// </summary>
        StripIllegalDots = 16,

        /// <summary>
        /// Limits the depth of the directory hierarchy and allows the implementation to truncate file names, convert them
        /// to upper case, strip illegal extra dots, and resolve name conflicts.
        /// </summary>
        Default = LimitDirectories | TruncateFileNames | UpperCaseFileNames | StripIllegalDots | ResolveNameConflicts,
        
        /// <summary>
        /// Does exactly what the ISO-9660 standard says, and doesn't modify file names in any way in order
        /// to get file names to fit in the allowed space.
        /// </summary>
        Strict = LimitDirectories
    }

    public enum Mode {
        Mode1,
        Mode2Form1,
        Mode2Form2
    }

    [Flags]
    public enum Extensions {
        None,
        RockRidge = 1,
        Joliet = 2,
        Udf = 4,
        ElTorito = 8,
        Apple = 16
    }

    public enum VolumeDescriptorType : byte {
        BootRecord = 0,
        Primary = 1,
        Supplementary = 2,
        VolumePartitionDescriptor = 3,
        // 4 - 254 are reserved
        Terminator = 255
    }

    public enum ElToritoPlatformId : byte {
        X86 = 0,
        PowerPC = 1,
        Mac = 2
    }

    public enum ElToritoMediaType : byte {
        NoEmulation = 0,
        Diskette1Point2MB = 1,
        Diskette1Point44MB = 2,
        Diskette2Point88MB = 3,
        HardDisk = 4
    }

    public enum ElToritoSelectionCriteria : byte {
        None = 0,
        IbmLanguageAndVersionInformation = 1
    }

    [Flags]
    public enum FileFlags : byte {
        None = 0,
        Existence = 1, // Meaning it is visible to the user
        Directory = 2,
        AssociatedFile = 4,
        Record = 8,
        Protection = 16,
        MultiExtent = 128
    }

    public enum RecordFormat : byte {
        NotSpecified = 0,
        FixedLength = 1,
        VariableWithLittleEndianRcw = 2,
        VariableWithBigEndianRcw = 3,
    }

    public enum RecordAttributes : byte {
        PrecedeWithLFAndFollowWithCR = 0,
        // ECMA-119 does not seem to specify what part of ISO-1539 defines 
        // this, and I have no desire to go looking through 563 pages of 
        // the Fortran specification (and that's only ISO-1539-1)...
        Iso1539 = 1,
        ContainedInRecord = 2
    }
#endregion

    #region Disk Content Model
    public class DiskImage {
        public Volume PrimaryVolume { get; set; }
        public BootCatalog BootCatalog { get; set; }

        public List<Volume> SupplementaryVolumes { get; set; }

        public DiskImage() {
            SupplementaryVolumes = new List<Volume>();
        }
    }

    #region Volumes
    public class Volume {
        public string SystemIdentifier { get; set; }
        public string VolumeIdentifier { get; set; }

        // Some fields aren't known at the content model stage,
        // such as how many logical blocks are used by the volume,
        // locations of path tables, etc...

        public ushort VolumeSetSize { get; set; }
        public ushort VolumeSequenceNumber { get; set; }
        public ushort LogicalBlockSize { get; set; }
        public Directory RootDirectory { get; set; }

        public string VolumeSetIdentifier { get; set; }
        public string PublisherIdentifier { get; set; }
        public string DataPreparerIdentifier { get; set; }
        public string ApplicationIdentifier { get; set; }
        public string CopyrightFileIdentifier { get; set; }
        public string AbstractFileIdentifier { get; set; }
        public string BibliographicFileIdentifier { get; set; }

        public DateTime? CreationDateTime { get; set; }
        public DateTime? ModificationDateTime { get; set; }
        public DateTime? ExpirationDateTime { get; set; }
        public DateTime? EffectiveDateTime { get; set; }

        public byte[] ApplicationSpecificData { get; set; }

        public Volume() {
            LogicalBlockSize = 512; // TODO Same as logical sector size?
            VolumeSetSize = 1;
            VolumeSequenceNumber = 1;
        }

        /// <summary>
        /// Loads the volume contents from the file system.
        /// </summary>
        public Volume(string path, bool useDirectoryDates, bool useUtcDates, bool setHiddenFlag) : this() {
            if (path == null)
                throw new ArgumentNullException("path");

            var dir = new IO.DirectoryInfo(path);

            if (useDirectoryDates) {
                if (useUtcDates) {
                    CreationDateTime = dir.CreationTimeUtc;
                    ModificationDateTime = dir.LastWriteTimeUtc;
                } else {
                    CreationDateTime = dir.CreationTime;
                    ModificationDateTime = dir.LastWriteTime;
                }
            }

            RootDirectory = new Directory(dir, setHiddenFlag);
        }
    }

    public abstract class FileSystemObject {
        public DateTime? RecordingDateTime { get; set; }
        public bool Hidden { get; set; }
        public bool AssociatedFile { get; set; }
        public bool Record { get; set; }
        public bool Protection { get; set; }
        public bool MultiExtent { get; set; }
        
        public string Name { get; set; }

        public string MappedName { get; set; }
        public byte[] MappedIdentifier { get; set; }
    }

    public class Directory : FileSystemObject {
        public List<FileSystemObject> Entries { get; set; }

        public Directory() {
            Entries = new List<FileSystemObject>();
        }

        public Directory(IO.DirectoryInfo dir, bool setHiddenFlag = false) : this() {
            Name = dir.Name;
            if (setHiddenFlag)
                Hidden = dir.Attributes.HasFlag(IO.FileAttributes.Hidden);

            foreach(var dirInfo in dir.EnumerateDirectories())
                Entries.Add(new Directory(dirInfo, setHiddenFlag));

            foreach(var fileInfo in dir.EnumerateFiles())
                Entries.Add(new File(fileInfo, setHiddenFlag));
        }
    }

    public class File : FileSystemObject {
        public string FileSystemPath { get; set; }
        public uint DataLength { get; set; }

        public File() {
        }

        public File(IO.FileInfo fileInfo, bool setHiddenFlag = false) : this() {
            Name = fileInfo.Name;
            FileSystemPath = fileInfo.FullName;

            if (setHiddenFlag)
                Hidden = fileInfo.Attributes.HasFlag(IO.FileAttributes.Hidden);

            DataLength = checked((uint)fileInfo.Length);
        }
    }
    #endregion

    #region El Torito
    public class BootCatalog {
        // Fields for the validation entry
        public ElToritoPlatformId PlatformId { get; set; }
        public string IdString { get; set; }

        public BootCatalogEntry InitialEntry { get; set; }

        public List<BootSection> Sections { get; set; }

        public BootCatalog() {
            Sections = new List<BootSection>();
        }
    }

    public class BootSection {
        public ElToritoPlatformId PlatformId { get; set; }

        public List<BootCatalogEntry> Entries { get; set; }
    }

    public class BootCatalogEntry {
        public bool Bootable { get; set; }
        public ElToritoMediaType MediaType { get; set; }
        public ushort LoadSegment { get; set; }
        public byte SystemType { get; set; } // This should be a copy of byte 5 from the Partition Table found in the boot image.
        public ushort SectorCount { get; set; }

        public byte[] Data { get; set; }

        // Extension data
        public byte[] VendorUniqueSelectionCriteria { get; set; }
    }
    #endregion
    #endregion

    public class DiskImageWriter : IDisposable {
        readonly string path;
        readonly IO.Stream stream;

        readonly Mode mode;
        readonly CompatibilityLevel compatibilityLevel;
        readonly CompatibilityFlags compatibilityFlags;
        readonly Extensions extensions;

        public DiskImageWriter(
            string path, 
            Mode mode = Mode.Mode1, 
            CompatibilityLevel compatibilityLevel = CompatibilityLevel.Level1, 
            CompatibilityFlags compatibilityFlags = CompatibilityFlags.Default, 
            Extensions extensions = Extensions.None) 
        {
            if (path == null)
                throw new ArgumentNullException("path");
            this.path = path;

            this.mode = mode;
            this.compatibilityLevel = compatibilityLevel;
            this.compatibilityFlags = compatibilityFlags;
            this.extensions = extensions;
            ValidateOptions();

            string dirName = IO.Path.GetDirectoryName(path);
            Debug.Assert(dirName != null, "dirName != null");
            if (!IO.Directory.Exists(dirName))
                IO.Directory.CreateDirectory(dirName);

            stream = IO.File.Open(path, IO.FileMode.Create, IO.FileAccess.ReadWrite, IO.FileShare.None);
        }

        public string Path {
            get { return path; }
        }

        private void ValidateOptions() {
            if (mode != Mode.Mode1)
                throw new NotSupportedException("Only ISO 9660 Mode 1 images are supported");
            if ((extensions & Extensions.Apple) == Extensions.Apple)
                throw new NotSupportedException("The Apple ISO 9660 extensions are not supported");
            if ((extensions & Extensions.Udf) == Extensions.Udf)
                throw new NotSupportedException("UDF is not supported");
        }

        public void Dispose() {
            stream.Dispose();
        }

        #region Constants
        const int bytesInSector = 2048;
        const int sectorsInSystemArea = 16;
        readonly int bytesInLogicalBlock = bytesInSector; // TODO Allow changing this...

        const int bytesInVolumeDescriptorHeader = 7;
        const int bytesInVolumeDescriptorData = bytesInSector - bytesInVolumeDescriptorHeader;

        const string dCharacters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        const string dCharactersAndSeparators = ".0123456789;ABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        const string aCharacters = " !=%&'()*+,-./0123456789:;<=>?ABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        const byte fileNameExtensionSeparator = 0x2E; // .
        const byte fileNameVersionSeparator = 0x3B; // ;
        #endregion

        #region Location mappings
        readonly Dictionary<Volume, VolumeLocationMap> volumes = new Dictionary<Volume, VolumeLocationMap>();
        readonly Dictionary<Directory, DirectoryLocationMap> directories = new Dictionary<Directory, DirectoryLocationMap>();
        readonly Dictionary<File, FileLocationMap> files = new Dictionary<File, FileLocationMap>();
        uint? sectorOfBootRecord;        

        /// <summary>
        /// Volume descriptors take up one logical sector and are located at the start of a logical sector.
        /// </summary>
        class VolumeLocationMap {
            public bool Written { get; set; }
            
            public uint SectorOfVolumeDescriptor { get; private set; }

            public uint? SectorOfTypeLPathTable { get; set; }
            public uint? SectorOfTypeMPathTable { get; set; }
            public uint PathTableSize { get; set; }
            public uint NumberOfLBs { get; set; }

            public VolumeLocationMap(uint sectorOfDescriptor) {
                SectorOfVolumeDescriptor = sectorOfDescriptor;
            }
        }

        /// <summary>
        /// Directory extents are located at the start of the first logical block in a logical sector.
        /// </summary>
        class DirectoryLocationMap {
            public bool Written { get; set; }

            public uint ExtentSector { get; private set; }
            public uint SectorCount { get; private set; }
            public uint DataLength { get; private set; }

            public DirectoryLocationMap(uint extentSector, uint dataLength, uint sectorCount) {
                ExtentSector = extentSector;
                SectorCount = sectorCount;
                DataLength = dataLength;
            }
        }

        class FileLocationMap {
            public bool Written { get; set; }

            public uint ExtentSector { get; private set; }
            public uint SectorCount { get; private set; }
            public uint DataLength { get; private set; }

            public FileLocationMap(uint sectorOfDescriptor, uint dataLength, uint sectorCount) {
                ExtentSector = sectorOfDescriptor;
                DataLength = dataLength;
                SectorCount = sectorCount;
            }
        }

        VolumeLocationMap GetVolumeLocationMap(Volume volume) {
            if (volume == null)
                throw new ArgumentNullException("volume");

            VolumeLocationMap map;
            if (volumes.TryGetValue(volume, out map))
                return map;

            throw InvalidOperationException("The given volume ({0}) has not been allocated a volume descriptor block, therefore it cannot be written to the disk image", volume.VolumeIdentifier ?? "Unidentified");
        }

        DirectoryLocationMap GetDirectoryLocationMap(Directory directory) {
            if (directory == null)
                throw new ArgumentNullException("directory");

            DirectoryLocationMap map;
            if (directories.TryGetValue(directory, out map))
                return map;

            throw InvalidOperationException("The given directory ({0}) has not been allocated a directory extent, therefore it cannot be written to the disk image", directory.Name ?? "Unidentified");
        }

        FileLocationMap GetFileLocationMap(File file) {
            if (file == null)
                throw new ArgumentNullException("file");

            FileLocationMap map;
            if (files.TryGetValue(file, out map))
                return map;

            throw InvalidOperationException("The given file ({0}) has not been allocated a file extent, therefore it cannot be written to the disk image", file.Name ?? "Unidentified");
        }
        #endregion

        #region Addressing code
        uint GetCurrentLogicalSector() {
            Debug.Assert(stream.Position % bytesInSector == 0, "Expected to be at the start of a sector");
            return checked((uint)(stream.Position / bytesInSector));
        }

        uint GetStartOfCurrentLogicalSector() {
            return checked((uint)(stream.Position - (stream.Position % bytesInSector)));
        }

        uint GetCurrentLba() {
            Debug.Assert(stream.Position % bytesInLogicalBlock == 0, "Expected to be at the start of a logical block");
            return checked((uint)(stream.Position / bytesInLogicalBlock));
        }

        uint GetStartOfCurrentLB() {
            return checked((uint)(stream.Position / bytesInLogicalBlock));
        }

        void SeekToLba(uint lba, uint byteOffset = 0) {
            stream.Position = lba * bytesInLogicalBlock + byteOffset;
        }

        void SeekToSector(uint sector, uint byteOffset = 0) {
            stream.Position = sector * bytesInSector + byteOffset;
        }

        bool AtStartOfSector() {
            return stream.Position % bytesInSector == 0;
        }

        void AssertStartOfSector(string message) {
            Debug.Assert(AtStartOfSector(), message);
        }

        void SeekToNextSector() {
            stream.Position = GetStartOfCurrentLogicalSector() + bytesInSector;
        }

        uint BytePositionFromLba(uint lba, uint byteOffset = 0) {
            return checked((uint)(lba * bytesInLogicalBlock + byteOffset));
        }

        uint BytePositionFromSector(uint sector, uint byteOffset = 0) {
            return sector * bytesInSector + byteOffset;
        }

        uint LbaFromSector(uint sector) {
            return checked((uint)((bytesInSector / bytesInLogicalBlock) * sector));
        }
           
        void PreservingLocation(Action action) {
            var pos = stream.Position;
            try {
                action();
            } finally {
                stream.Position = pos;
            }
        }

        uint SectorsNeededForByteCount(uint bytes) {
            if (bytes == 0)
                return 0;
            return 1 + ((bytes - 1) / bytesInSector);
        }
        #endregion

        #region Name mapping
        const int maximumDirectoryNesting = 8;
        const int maximumPathLength = 255;
        const int maxFileNameAndExtensionLength = 30;

        /// <summary>
        /// Validates file names, generates 8.3 file names (if necessary), records the new names, and checks
        /// that the directory nesting limit and maximum path length limit will not be exceeded.
        /// </summary>
        /// <param name="volume"></param>
        void GenerateNames(Volume volume) {
            if (volume == null)
                throw new ArgumentNullException("volume");
            if (volume.RootDirectory == null)
                throw ArgumentException("volume", "The given volume ({0}) cannot be processed because does not have a root directory", volume.VolumeIdentifier ?? "Unidentified");
            
            GenerateNames(volume, volume.RootDirectory, 1, 0);
        }

        void GenerateNames(Volume volume, Directory directory, int levelInDirectoryHierarchy, int currentPathLength) {
            if (volume == null)
                throw new ArgumentNullException("volume");
            if (directory == null)
                throw new ArgumentNullException("directory");

            if (directory.Entries.Any(e => e is Directory) && levelInDirectoryHierarchy == maximumDirectoryNesting)
                throw InvalidOperationException("Cannot process this volume ({0}) because a directory ({1}) has subdirectories that would exceed the directory nesting limit of {2}.", volume.VolumeIdentifier ?? "Unidentified", directory.Name ?? "Unidentified", maximumDirectoryNesting);

            foreach (var child in directory.Entries.Where(e => !e.AssociatedFile)) {
                GenerateName(child, directory, currentPathLength);
                if (child is Directory)
                    GenerateNames(volume, child as Directory, levelInDirectoryHierarchy + 1, child.MappedIdentifier.Length + 1);
            }

            // We have to generate names for primary files before names can be assigned to associated files,
            // because the associated files must have the same name as the primary file.
            foreach (var child in directory.Entries.Where(e => !e.AssociatedFile)) {

            }

            // TODO Sort the file list and directory list so according to ISO-9660 9.3
        }

        void GenerateName(FileSystemObject child, Directory context, int currentPathLength) {
            if (child == null)
                throw new ArgumentNullException("child");
            if (context == null)
                throw new ArgumentNullException("context");

            if (child.Name == null)
                throw ArgumentException("child", "Cannot process a {0} in this directory ({1}) because it does not have a name", child.GetType().Name.ToLowerInvariant(), context.Name);
            if (child.MappedName != null)
                throw InvalidOperationException("Cannot generate a name mapping {0} ({1}) because a mapping has already been generated", child.GetType().Name.ToLowerInvariant(), child.Name);

            if (child.AssociatedFile) {
                // Find a file which has the same canonical name, then use its mapped name as the 
                // associated file's mapped name.
                var primary = context.Entries.FirstOrDefault(e => e.Name == child.Name && !e.AssociatedFile);
                if (primary == null)
                    throw ArgumentException("child", "Cannot process this associated file record ({0}) because the primary file record cannot be found in the directory ({1})", child.Name, context.Name);
            
                child.MappedName = primary.MappedName;
                child.MappedIdentifier = primary.MappedIdentifier;
                return;
            }

            int nameLen, extLen;
            string name = TryTruncate(child.Name, child is Directory, out nameLen, out extLen);
            bool isTruncated = name != child.Name;

            // Take off the extension to generate the tilde name.
            string extPart = name.Substring(nameLen);
            string namePart = name.Substring(0, nameLen);
            Func<string, bool> acceptableName = candidate => !context.Entries.Any(e => !e.AssociatedFile && e.MappedName == candidate + extPart);

            string tildeName = GenerateTildeName(namePart, nameLen, true, child is Directory, child.Name, isTruncated, 4, acceptableName);

            // If that didn't work, try the more severe option, which is two characters from the name,
            // 4 hash characters based on the file name, and a tilde part. 
            if (tildeName == null) {
                int charsToTake = nameLen - 6;
                string baseName = charsToTake > 0 && charsToTake < namePart.Length ? namePart.Substring(0, charsToTake) : namePart;
                ushort hash = TildeFileNameHash(namePart);
                tildeName = GenerateTildeName(baseName + hash.ToString("X4", CultureInfo.InvariantCulture), baseName.Length + 6, false, child is Directory, child.Name, isTruncated, 9, acceptableName);

                if (tildeName == null)
                    throw ArgumentException("name", "The directory ({0}) cannot be processed because it contains too many files or directories with names that clash with ({1})", context.Name, child.Name);
            }

            // Now add the extension and version back on.
            tildeName += extPart;

            // Now, at last, check that the name doesn't exceed the maximum path length of 255.
            if (currentPathLength + tildeName.Length > maximumPathLength)
                throw ArgumentException("child", "Cannot process this {0} ({1}) because its full path would exceed the maximum path size of {2} bytes", child.GetType().Name.ToLowerInvariant(), child.Name, maximumPathLength);

            child.MappedName = tildeName;
            child.MappedIdentifier = ascii.GetBytes(tildeName);
            Debug.Assert(child.MappedIdentifier.Length == child.MappedName.Length, "ASCII Encoding generated a multi-byte output", "The name in question is {0}", child.MappedName);
        }

        // The actual algorithm used by Microsoft® Windows™ is undocumented, so we'll just use 
        // .NET's GetHashCode and hope it's good enough.
        ushort TildeFileNameHash(string name) {
            return (ushort)name.GetHashCode();
        }

        string GenerateTildeName(string name, int maximumLength, bool tryWithoutTilde, bool isDirectory, string originalName, bool isTruncated, int tries, Func<string, bool> acceptableName) {
            // We try four tilde names, then if all are taken, we just give up.
            for (int i = tryWithoutTilde ? 0 : 1; i <= tries; i++) {
                string tildeName = name;
                if (i > 0)
                    tildeName = (name.Length > maximumLength - 2 ? name.Substring(0, maximumLength - 2) : name) + "~" + i.ToString(CultureInfo.InvariantCulture);

                // Check for clashes, even if the name hasn't been shortened, because it's possible some
                // other name has been shortened and now will clash with this name...
                if (acceptableName(tildeName))
                    return tildeName;

                if (!compatibilityFlags.HasFlag(CompatibilityFlags.ResolveNameConflicts)) {
                    if (isTruncated)
                        throw ArgumentException("child", "The {0} ({1}) cannot be processed because its truncated name ({2}) clashes with an existing file or directory name, and the ResolveNameConflicts option is not enabled", isDirectory ? "directory" : "file", originalName, name);
                    throw ArgumentException("child", "The {0} ({1}) cannot be processed because its name clashes with an existing file or directory name, and the ResolveNameConflicts option is not enabled", isDirectory ? "directory" : "file", originalName);
                }
            }

            // Failed to generate an acceptable name in the given amount of tries.
            return null;
        }

        /// <summary>
        /// Truncates file and directory names, depending on the compatibility flags defined. The algorithm
        /// for truncating files to 8.3 notation is based loosely on that described on Wikipedia.
        /// </summary>
        string TryTruncate(string name, bool isDirectory, out int nameLength, out int extensionLength) {
            bool level1 = compatibilityLevel == CompatibilityLevel.Level1;
            bool allowTruncate = compatibilityFlags.HasFlag(CompatibilityFlags.TruncateFileNames);
            bool canStripDots = compatibilityFlags.HasFlag(CompatibilityFlags.StripIllegalDots);

            // Although it's technically not truncating, we should do this too, if the compatibility
            // flags allow it. If the name isn't uppercase, and we are in Level 1 compatibility mode,
            // any disallowed characters will be stripped from the name.
            if (level1 && compatibilityFlags.HasFlag(CompatibilityFlags.UpperCaseFileNames))
                name = name.ToUpperInvariant();

            int nameLen = 0, extLen = 0, verIndex = 0;
            bool foundSep1 = false, foundSep2 = false;

            // Strip all invalid characters
            var sb = new StringBuilder();
            foreach (char currentChar in name) {
                bool ok = false;
                char c = currentChar;

                if (level1) {
                    if (dCharactersAndSeparators.IndexOf(c) >= 0) {
                        ok = true;
                    } else if (compatibilityFlags.HasFlag(CompatibilityFlags.UpperCaseFileNames)) {
                        c = char.ToUpperInvariant(c);
                        if (dCharactersAndSeparators.Contains(c))
                            ok = true;
                    }
                } else {
                    ok = true; // Allow any character in the file name
                }
                
                // Applies to all compatibility levels
                if ((byte)c == fileNameExtensionSeparator) {
                    if (foundSep1) {
                        if (foundSep2 || !canStripDots)
                            throw ArgumentException("name", "The name \"{0}\" is invalid because only one '.' is allowed in ISO-9660 Level 1 compatibility mode", name);

                        for (int i = 0; i < sb.Length; ++i)
                            if (sb[i] == (char)fileNameExtensionSeparator)
                                sb.Remove(i, 1);

                        extLen = 0;
                        nameLen = sb.Length;
                    }

                    if (isDirectory) {
                        if (!canStripDots)
                            throw ArgumentException("name", "The name \"{0}\" is invalid because '.' is not allowed in directory names in ISO-9660 Level 1 compatibility mode", name);

                        ok = false;
                    } else {
                        foundSep1 = true;
                    }
                }

                // Applies to all compatibility levels
                if ((byte)c == fileNameVersionSeparator) {
                    if (foundSep2)
                        throw ArgumentException("name", "The name \"{0}\" is invalid because only one ';' is allowed in ISO-9660 Level 1 compatibility mode", name);
                    if (!foundSep1)
                        throw ArgumentException("name", "The name \"{0}\" is invalid because a '.' was not found before the ';'", name);
                    foundSep2 = true;
                    verIndex = sb.Length + 1;
                }

                if (ok) {
                    if (!foundSep1)
                        nameLen++;
                    else if (!foundSep2 && (byte)c != fileNameExtensionSeparator)
                        extLen++;
                    sb.Append(c);
                }
            }

            string initialName = name; // For use in errors

            // It's possible that there's no allowed characters in the name!
            if (sb.Length == 0) {
                if (name.Length > 0)
                    throw ArgumentException("name", "The name \"{0}\" is invalid because it does not contain any allowed characters, and the UpperCaseFileNames flag is not set", initialName);
                throw ArgumentException("name", "Cannot process this file because its name is of zero length");
            }

            // Check the file version number, which can either be omitted or a number between 1 and 32767 inclusive
            if (foundSep2) {
                const int minVersion = 1, maxVersion = 32767;
                int version;
                if (int.TryParse(sb.ToString(verIndex, sb.Length - verIndex), NumberStyles.Integer, CultureInfo.InvariantCulture, out version)) {
                    if (version < minVersion || version > maxVersion)
                        throw ArgumentException("name", "The name \"{0}\" is invalid because the file version portion in ISO-9660 Level 1 compliance mode must be a number between 1 and 32767. The version for this file is {1}", initialName, version);
                } else {
                    throw ArgumentException("name", "The name \"{0}\" is invalid because the file version portion in ISO-9660 Level 1 compliance mode must be a number between 1 and 32767", initialName);
                }
            }

            if (level1) {
                // Ensure proper 8.3 compliance or just 8-compliance for directories
                if (nameLen > 8) {
                    if (!allowTruncate)
                        throw ArgumentException("name", "The name \"{0}\" is invalid because the maximum length of the file name portion in ISO-9660 Level 1 compliance mode is 8 characters", initialName);
                    
                    sb.Remove(8, nameLen - 8);
                    nameLen = 8;
                }

                if (extLen > 3) {
                    if (!allowTruncate)
                        throw ArgumentException("name",
                            "The name \"{0}\" is invalid because the maximum length of the file extension portion in ISO-9660 Level 1 compliance mode is 3 characters",
                            initialName);
                    
                    sb.Remove(nameLen + 1 + extLen, extLen - 3);
                    extLen = 3;
                }
            }

            name = sb.ToString();

            // Ensure file name + extension portion does not exceed 30 in all compatibility levels
            if (!isDirectory && nameLen + extLen > maxFileNameAndExtensionLength) {
                if (!allowTruncate)
                    throw ArgumentException("name", "The file name and extension portion of the given file name ({0}) is longer than the maximum length given by section 7.5.1 of the ISO-9660 specification (which is 30), and the TruncateFileNames option is not enabled.", initialName);

                string dotOnwards = name.Substring(nameLen); // Include the '.'
                if (extLen > maxFileNameAndExtensionLength) {
                    name = dotOnwards.Substring(0, maxFileNameAndExtensionLength) + dotOnwards.Substring(1 + extLen);
                    extLen = maxFileNameAndExtensionLength;
                } else {
                    name = name.Substring(0, maxFileNameAndExtensionLength - extLen) + dotOnwards;
                }

                nameLen = maxFileNameAndExtensionLength - extLen;
            }

            if (nameLen == 0 && extLen == 0)
                throw ArgumentException("name", "The file name ({0}) is invalid because neither the file name nor the file extension is specified, contrary to section 7.5.1 of the ISO-9660 specification.", initialName);

            // Ensure length does not exceed 31 in all Compatibility Level 2 and above
            const int maxDirectoryIdentifierLength = 31;
            if (isDirectory && name.Length > maxDirectoryIdentifierLength) {
                if (!allowTruncate)
                    throw ArgumentException("name", "The given directory name ({0}) is longer than the maximum length given by section 7.6.3 of the ISO-9660 specification (which is {1}), and the TruncateFileNames option is not enabled.", initialName, maxDirectoryIdentifierLength);

                nameLen -= (name.Length - maxDirectoryIdentifierLength);
                name = name.Substring(0, maxDirectoryIdentifierLength);
            }

            nameLength = nameLen;
            extensionLength = extLen;

            return name;
        }
        #endregion

        #region Allocation Code
        void AllocateVolumeDescriptor(Volume volume) {
            if (volume == null)
                throw new ArgumentNullException("volume");
            if (volumes.ContainsKey(volume))
                throw InvalidOperationException("A volume descriptor for this volume ({0}) has already been allocated", volume.VolumeIdentifier ?? "Unidentified");

            volumes.Add(volume, new VolumeLocationMap(GetCurrentLogicalSector()));
            SeekToNextSector();
        }

        void AllocateDirectoryExtent(Directory directory) {
            if (directory == null)
                throw new ArgumentNullException("directory");
            if (directories.ContainsKey(directory))
                throw InvalidOperationException("An extent for this directory ({0}) has already been allocated", directory.Name ?? "null");
            if (!AtStartOfSector())
                throw InvalidOperationException("Cannot allocate an extent for this directory ({0}) because the extent must be allocated at the start of a sector. The current location is ({1}) bytes after a sector boundary.", directory.Name ?? "null", stream.Position % bytesInSector);

            uint requiredBytes = MeasureDirectory(directory);
            uint requiredSectors = SectorsNeededForByteCount(requiredBytes);
            if (requiredSectors == 0)
                requiredSectors = 1;

            // ISO-9660 6.8.1.3
            requiredBytes = requiredSectors * bytesInSector;

            var dlm = new DirectoryLocationMap(GetCurrentLogicalSector(), requiredBytes, requiredSectors);
            directories.Add(directory, dlm);

            SeekToSector(dlm.ExtentSector + dlm.SectorCount);

            // Now allocate all the children.
            foreach (var dir in directory.Entries.OfType<Directory>())
                AllocateDirectoryExtent(dir);

            foreach (var file in directory.Entries.OfType<File>())
                AllocateFileExtent(file);
        }

        void AllocateFileExtent(File file) {
            if (file == null)
                throw new ArgumentNullException("file");
            if (files.ContainsKey(file))
                throw InvalidOperationException("An extent for this file ({0}) has already been allocated", file.Name ?? "null");
            if (!AtStartOfSector())
                throw InvalidOperationException("Cannot allocate an extent for this file ({0}) because the extent must be allocated at the start of a sector. The current location is ({1}) bytes after a sector boundary.", file.Name ?? "null", stream.Position % bytesInSector);

            uint sectors = SectorsNeededForByteCount(file.DataLength);

            // If a file has length zero, the length of the extent will be set to zero as well.
            // Currently the code does not allocate a sector for the file, as common sense 
            // would dictate.

            var flm = new FileLocationMap(sectors > 0 ? GetCurrentLogicalSector() : 0, file.DataLength, sectors);
            files.Add(file, flm);
            SeekToSector(GetCurrentLogicalSector() + sectors);
        }

        void AllocateBootRecord(BootCatalog catalog) {
            if (sectorOfBootRecord.HasValue)
                throw InvalidOperationException("You may not allocate this boot record ({0}) because a boot record has already been allocated", catalog.IdString ?? "Unidentified publisher");

            sectorOfBootRecord = GetCurrentLogicalSector();
            SeekToNextSector();
        }
        #endregion

        #region Logical Writing Code
        public void WriteDiskImage(DiskImage image) {
            SeekToSector(sectorsInSystemArea);

            // While ISO-9660 does not seem to specify a particular order, the El Torito
            // specification states that the first two sectors of the Data Area must be
            // the Primary Volume Descriptor followed by the Boot Record.
            if (image.PrimaryVolume == null)
                throw ArgumentException("image", "Cannot write this disk image because no primary volume is defined");

            GenerateNames(image.PrimaryVolume);
            foreach (var volume in image.SupplementaryVolumes)
                GenerateNames(volume);

            AllocateVolumeDescriptor(image.PrimaryVolume);
            if (image.BootCatalog != null)
                AllocateBootRecord(image.BootCatalog);

            if (image.SupplementaryVolumes != null)
                foreach(var volume in image.SupplementaryVolumes)
                    AllocateVolumeDescriptor(volume);

            WriteVolumeDescriptorSetTerminator();

            if (image.BootCatalog != null)
                WriteBootCatalog(image.BootCatalog);

            WriteVolume(image.PrimaryVolume, isPrimary: true);

            if (image.SupplementaryVolumes != null)
                foreach (var volume in image.SupplementaryVolumes)
                    WriteVolume(volume, isPrimary: false);

            stream.Seek(0, IO.SeekOrigin.End);
            if (!AtStartOfSector()) {
                SeekToNextSector();
                WriteByte(0);
            }
        }

        void WriteVolume(Volume volume, bool isPrimary) {
            if (volume == null)
                throw new ArgumentNullException("volume");
            if (volume.RootDirectory == null)
                throw ArgumentException("volume", "The given volume ({0}) does not have a root directory, and thus cannot be written to the disk image.", volume.VolumeIdentifier ?? "Unidentified");

            // XXX HACK While smaller logical blocks doesn't currently work
            volume.LogicalBlockSize = checked((ushort)bytesInLogicalBlock);

            var vlm = GetVolumeLocationMap(volume);

            var initialBP = stream.Position;

            AllocateDirectoryExtent(volume.RootDirectory);
            WriteDirectoryExtent(volume.RootDirectory, null);
            stream.Seek(0, IO.SeekOrigin.End);
            
            // TODO Need to change the allocation code to keep a pointer
            //      to where new data is to be allocated
            SeekToNextSector();

            vlm.SectorOfTypeLPathTable = GetCurrentLogicalSector();
            WritePathTable(volume, false);
            uint typeLSize = checked((uint)(stream.Position - BytePositionFromSector(vlm.SectorOfTypeLPathTable.Value)));
            
            if (!AtStartOfSector())
                SeekToNextSector();
            vlm.SectorOfTypeMPathTable = GetCurrentLogicalSector();
            WritePathTable(volume, true);
            uint typeMSize = checked((uint)(stream.Position - BytePositionFromSector(vlm.SectorOfTypeMPathTable.Value)));

            Debug.Assert(typeLSize == typeMSize, "Path table sizes are inconsistent", "Type L Size: {0}\nType M Size: {1}", typeLSize, typeMSize);
            vlm.PathTableSize = typeLSize;
            
            SeekToNextSector();
            vlm.NumberOfLBs = checked((uint)((stream.Position - initialBP) / bytesInLogicalBlock));
            
            vlm.Written = true;
            PreservingLocation(() => WriteVolumeDescriptor(volume, isPrimary));
        }
        #endregion

        #region ISO-9660 structures
        void WriteVolumeDescriptor(Volume volume, bool isPrimary) {
            if (volume == null)
                throw new ArgumentNullException("volume");
            if (volume.RootDirectory == null)
                throw ArgumentException("volume", "The given volume ({0}) must have a root directory", volume.VolumeIdentifier ?? "Unidentified");

            var vlm = GetVolumeLocationMap(volume);
            if (!vlm.SectorOfTypeLPathTable.HasValue)
                throw InvalidOperationException("The volume descriptor given volume ({0}) cannot be written because the type L path table for the volume has not been written", volume.VolumeIdentifier ?? "Unidentified");
            if (!vlm.SectorOfTypeMPathTable.HasValue)
                throw InvalidOperationException("The volume descriptor given volume ({0}) cannot be written because the type M path table for the volume has not been written", volume.VolumeIdentifier ?? "Unidentified");
            
            SeekToSector(vlm.SectorOfVolumeDescriptor);

            WriteVolumeDescriptorHeader(isPrimary ? VolumeDescriptorType.Primary : VolumeDescriptorType.Supplementary);

            WriteByte(0);
            WriteAString(volume.SystemIdentifier, 32, "System Identifier");
            WriteDString(volume.VolumeIdentifier, 32, "Volume Identifier");
            WriteZeroBytes(8);

            // Volume Space Size
            WriteBothEndianUInt32(vlm.NumberOfLBs);

            WriteZeroBytes(32);

            WriteBothEndianUInt16(volume.VolumeSetSize);
            WriteBothEndianUInt16(volume.VolumeSequenceNumber);
            
            Debug.Assert(volume.LogicalBlockSize == bytesInLogicalBlock, "Logical block size cannot currently be changed");
            WriteBothEndianUInt16(volume.LogicalBlockSize);

            // Path Table Size
            WriteBothEndianUInt32(SectorsNeededForByteCount(vlm.PathTableSize) * bytesInSector);

            WriteLittleEndianUInt32(LbaFromSector(vlm.SectorOfTypeLPathTable.Value));
            WriteZeroBytes(4); // TODO Optional Type L Path Table Location

            WriteBigEndianUInt32(LbaFromSector(vlm.SectorOfTypeMPathTable.Value));
            WriteZeroBytes(4); // TODO Optional Type M Path Table Location

            var rootDlm = GetDirectoryLocationMap(volume.RootDirectory);
            WriteDirectoryRecord(
                new byte[] { 0x00 },
                rootDlm.ExtentSector,
                rootDlm.DataLength,
                DateTime.Now,
                FlagsFor(volume.RootDirectory),
                volume.VolumeSequenceNumber);

            WriteDString(volume.VolumeSetIdentifier, 128, "Volume Set Identifier");
            WriteAString(volume.PublisherIdentifier, 128, "Publisher Identifier");
            WriteAString(volume.DataPreparerIdentifier, 128, "Data Preparer Identifier");
            WriteAString(volume.ApplicationIdentifier, 128, "Application Identifier");
            WriteFileIdentifier(volume.CopyrightFileIdentifier, 37, "Copyright File Identifier"); // TODO Check 8.3
            WriteFileIdentifier(volume.AbstractFileIdentifier, 37, "Abstract File Identifier"); // TODO Check 8.3
            WriteFileIdentifier(volume.BibliographicFileIdentifier, 37, "Bibliographic File Identifier"); // TODO Check 8.3

            // ISO-9660 only actually specifies how these are encoded for primary volumes, but
            // it is to be assumed that the same encoding applies to supplementary volumes also.
            WriteDateTimeForVolumeDescriptor(volume.CreationDateTime);
            WriteDateTimeForVolumeDescriptor(volume.ModificationDateTime);
            WriteDateTimeForVolumeDescriptor(volume.ExpirationDateTime);
            WriteDateTimeForVolumeDescriptor(volume.EffectiveDateTime);

            // File Structure Version
            WriteByte(1);

            WriteByte(0);
        }

        void WriteVolumeDescriptorHeader(VolumeDescriptorType type, byte version = 1) {
            Debug.Assert(stream.Position % bytesInSector == 0, "Volume descriptor must be placed at the start of a logical sector");
            Debug.Assert(!(type == VolumeDescriptorType.Terminator && stream.Position == bytesInSector * sectorsInSystemArea), message: "You must write at least a primary volume descriptor");

            Write((byte)type);
            WriteDString("CD001", 5, "Standard Identifier");
            Write(version);
        }

        // TODO ISO-9660 6.8.1
        //      "Each Directory Record shall end in the Logical Sector in which it begins."
        //      Potentially have to add extra paddings to system use area of items immediately
        //      preceding the sector boundary.
        #region Directory Record
        const int bytesInBaseDirectoryRecord = 33;

        uint MeasureDirectory(Directory directory) {
            if (directory == null)
                throw new ArgumentNullException("directory");

            uint total = 0;

            // Account for the space that will be taken up by the pointers to 
            // itself and its parent directory.
            total += MeasureDirectoryRecord(new byte[1], true) * 2;

            // ReSharper disable LoopCanBeConvertedToQuery
            // (Doing so causes a compile error because Enumerable.Sum does not have an overload for uints...)
            foreach (var child in directory.Entries)
                total += MeasureDirectoryRecord(child.MappedIdentifier, child is Directory);
            // ReSharper restore LoopCanBeConvertedToQuery

            return total;
        }

        uint MeasureDirectoryRecord(byte[] identifier, bool isDirectory) {
            if (identifier == null)
                throw new ArgumentNullException("identifier");

            uint length = bytesInBaseDirectoryRecord;
            length += checked((uint)identifier.Length);
            if (length % 2 == 1)
                length++;

            return length;
        }

        /// <summary>
        /// Writes all the directory records for a directory and its files and subdirectories.
        /// </summary>
        /// <param name="directory">The directory to write.</param>
        /// <param name="parent">The <c>DirectoryLocationMap</c> of the parent directory, which
        /// contains information about the location of the parent directory's extent and the length
        /// of its data, <c>null</c> if the directory is the root directory.</param>
        /// <returns>The <c>DirectoryLocationMap</c> of the directory.</returns>
        DirectoryLocationMap WriteDirectoryExtent(Directory directory, DirectoryLocationMap parent) {
            if (directory == null)
                throw new ArgumentNullException("directory");

            var dlm = GetDirectoryLocationMap(directory);

            uint sector = dlm.ExtentSector;
            SeekToSector(sector);
            
            // We don't yet know the data length of this directory, however we can go back
            // and set it once we do know it... we just need to know where to write the 
            // data length to once we're finished

            const byte fileIdentifierOfCurrentDirectory = 0;
            const byte fileIdentifierOfParentDirectory = 1;

            // Write directory record for the current directory.
            // TODO Get flags/time zone from user/settings
            WriteDirectoryRecord(
                new byte[] { fileIdentifierOfCurrentDirectory }, 
                sector, 
                dlm.DataLength,
                DateTime.Now,
                FileFlags.Directory,
                1);

            // Write parent directory record. If the sector of the parent directory isn't 
            // specified then this directory is the root directory, therefore its '..'
            // record points to itself.
            WriteDirectoryRecord(
                new byte[] { fileIdentifierOfParentDirectory },
                parent != null ? parent.ExtentSector : sector,
                parent != null ? parent.DataLength: dlm.DataLength,
                DateTime.Now,
                FileFlags.Directory, 
                1);

            // Write all the children.
            // TODO Sort according to ISO-9660 section 9.3
            if (directory.Entries != null) {
                foreach (var dir in directory.Entries.OfType<Directory>()) {
                    DirectoryLocationMap childDlm;
                    
                    // Using PreservingLocation gives a warning about the behaviour of using foreach
                    // variables in a closure, so as a safety measure, I'm doing it manually.
                    var bp = stream.Position;
                    try {
                        childDlm = WriteDirectoryExtent(dir, dlm);
                    } finally {
                        stream.Position = bp;   
                    }

                    WriteDirectoryRecord(
                        dir.MappedIdentifier, 
                        childDlm.ExtentSector,
                        childDlm.DataLength,
                        DateTime.Now,
                        FlagsFor(dir),
                        1);
                }
            }

            if (directory.Entries != null)
                foreach (var file in directory.Entries.OfType<File>()) {
                    var bp = stream.Position;
                    FileLocationMap childFlm;
                    try {
                        childFlm = WriteFileExtent(file);
                    } finally {
                        stream.Position = bp;
                    }

                    WriteDirectoryRecord(
                        file.MappedIdentifier,
                        childFlm.ExtentSector,
                        childFlm.DataLength,
                        DateTime.Now,
                        FlagsFor(file),
                        1);
                }

            dlm.Written = true;
            return dlm;
        }

        static FileFlags FlagsFor(FileSystemObject entry) {
            if (entry == null)
                throw new ArgumentNullException("entry");

            var flags = entry is Directory ? FileFlags.Directory : FileFlags.None;
            if (entry.AssociatedFile)
                flags |= FileFlags.AssociatedFile;
            if (entry.Hidden)
                flags |= FileFlags.Existence;
            if (entry.MultiExtent)
                flags |= FileFlags.MultiExtent;
            if (entry.Protection)
                flags |= FileFlags.Protection;
            if (entry.Record)
                flags |= FileFlags.Record;

            return flags;
        }

        /// <summary>
        /// Can be used for either directories or files (which it is depends on
        /// the value of the <paramref name="flags"/> parameter).
        /// The ISO-9660 File Unit Size and Interleave Gap Size parameters are not
        /// supported.
        /// </summary>
        /// <param name="fileIdentifier">The name of the file or directory.</param>
        /// <param name="sectorOfExtent">The sector where the file or directory data is located.</param>
        /// <param name="dataLength">The length, in bytes, of the file or directory data.</param>
        /// <param name="recordingDateTime">The date and time of recording.</param>
        /// <param name="flags">Specifies whether it is a file or directory, visible or not, 
        /// an associated file, whether the structure of the information in the file is 
        /// specified by a record format (found in the Extended Attribute Record), 
        /// whether or not Owner and Group Identification are specified, and whether or not
        /// this is the final directory record for the file.</param>
        /// <param name="volumeSequenceNumber">The 1-based index of the volume within the
        /// volume set.</param>
        void WriteDirectoryRecord(byte[] fileIdentifier, uint sectorOfExtent, uint dataLength, DateTime recordingDateTime, FileFlags flags, ushort volumeSequenceNumber) {
            if (fileIdentifier == null)
                throw new ArgumentNullException("fileIdentifier");
            if (fileIdentifier.Length == 0)
                throw ArgumentException("fileIdentifier", "The file or directory identifier must contain at least one byte");

            var initialBP = stream.Position;

            int recordLength = fileIdentifier.Length + bytesInBaseDirectoryRecord;
            if (recordLength % 2 == 1)
                recordLength++; // Pad record to an even length

            if (recordLength > byte.MaxValue)
                throw ArgumentException("directory", "The file or directory name (\"{0}\") is too long to fit in the directory record. The maximum length allowed by the ISO-9660 specification, in any compatibility level, is 31 bytes.", ascii.GetString(fileIdentifier));
        
            WriteByte((byte)recordLength);
            WriteZeroBytes(1); // Extended attribute record length -- zero for now
            WriteBothEndianUInt32(LbaFromSector(sectorOfExtent));
            WriteBothEndianUInt32(dataLength);

            WriteDateTimeForDirectoryRecord(recordingDateTime);
            WriteByte((byte)flags);
            WriteZeroBytes(2); // File Unit Size and Interleave Gap Size (only relevant for interleaved mode)

            WriteBothEndianUInt16(volumeSequenceNumber);

            WriteByte((byte)fileIdentifier.Length);
            Write(fileIdentifier);
            if ((bytesInBaseDirectoryRecord + fileIdentifier.Length) % 2 == 1)
                WriteZeroBytes(1); // Padding

            Debug.Assert(stream.Position == initialBP + recordLength, "WriteDirectoryRecord() wrote the wrong amount of bytes");

            // No "system use" bytes to write.
        }

        FileLocationMap WriteFileExtent(File file) {
            if (file == null)
                throw new ArgumentNullException("file");

            var flm = GetFileLocationMap(file);

            SeekToSector(flm.ExtentSector);
            using (var contents = IO.File.Open(file.FileSystemPath, IO.FileMode.Open)) {
                if (contents.Length > flm.SectorCount * bytesInSector)
                    throw InvalidOperationException("The given file ({0}) cannot be written to the disk image because its contents have changed during the execution of the program, and its size has increased from {1} bytes to {2} bytes.", file.Name, file.DataLength, contents.Length);

                contents.CopyTo(stream);
            }

            flm.Written = true;
            return flm;
        }
        #endregion

        #region El Torito Boot Catalog
        const int bytesInBootCatalogEntry = 32;
        const string elToritoSystemIdentifier = "EL TORITO SPECIFICATION";

        void WriteBootCatalog(BootCatalog catalog) {
            if (catalog == null)
                throw new ArgumentNullException("catalog");
            if (catalog.InitialEntry == null)
                throw ArgumentException("catalog", "The boot catalog must have an initial/default entry");
            Debug.Assert(sectorOfBootRecord.HasValue, "Boot sector must be allocated before writing the boot catalog");
            
            var sectorOfCatalog = GetCurrentLogicalSector();

            // Write boot record
            PreservingLocation(delegate {
                SeekToSector(sectorOfBootRecord.Value);
                WriteBootRecord(sectorOfCatalog);
            });

            // Write boot catalog
            uint sectorToWrite = GetCurrentLogicalSector() + 1; 

            WriteBootCatalogValidationEntry(catalog);

            uint initialEntryDataSector = sectorToWrite;
            PreservingLocation(() => WriteBootSectorData(catalog.InitialEntry.Data, ref sectorToWrite));

            WriteBootCatalogEntry(catalog.InitialEntry, initialEntryDataSector);            

            // TODO Write additional boot catalog entries

            SeekToSector(sectorToWrite);
        }

        void WriteBootSectorData(byte[] data, ref uint sector) {
            SeekToSector(sector);

            uint sectorsOfData = SectorsNeededForByteCount((uint)data.Length);
            if (sectorsOfData < 1)
                sectorsOfData = 1;

            stream.Write(data, 0, data.Length);

            sector += sectorsOfData;
        }

        void WriteBootCatalogValidationEntry(BootCatalog catalog) {
            if (catalog == null)
                throw new ArgumentNullException("catalog");

            AssertStartOfSector("Boot catalog must be placed at the start of a logical sector");

            // Write validation entry
            WriteByte(1);
            WriteByte((byte)catalog.PlatformId);
            WriteZeroBytes(2);
            WriteAString(catalog.IdString, 24, "ID String");
            WriteZeroBytes(2); // The checksum will go here
            WriteByte(0x55);
            WriteByte(0xAA);

            // Read back the bytes we have written
            stream.Seek(-bytesInBootCatalogEntry, IO.SeekOrigin.Current);

            var validationEntry = new byte[bytesInBootCatalogEntry];
            stream.Read(validationEntry, 0, validationEntry.Length);

            uint sum = 0;
            for (int i = 0; i < validationEntry.Length - 1; i += 2) {
                var lsb = validationEntry[i];
                var msb = validationEntry[i + 1];
                sum += lsb;
                sum += (uint)(msb << 8);
            }

            // Write the new checksum. The checksum is defined such that
            // the sum of all the 16-bit words in the record is 0.
            stream.Seek(-4, IO.SeekOrigin.Current);
            WriteLittleEndianUInt16((ushort)(65536 - sum));
            stream.Seek(2, IO.SeekOrigin.Current);
        }

        void WriteBootCatalogEntry(BootCatalogEntry entry, uint dataSector) {
            if (entry == null)
                throw new ArgumentNullException("entry");

            Debug.Assert(stream.Position % bytesInBootCatalogEntry == 0, string.Format("Boot catalog entry must be placed on a {0}-byte boundary", bytesInBootCatalogEntry));

            WriteByte(entry.Bootable ? (byte)0x88 : (byte)0);
            WriteByte((byte)entry.MediaType);
            WriteLittleEndianUInt16(entry.LoadSegment); // Will be 7C0 if set to zero
            WriteByte(entry.SystemType); // System type (spec says this should be byte 5 of the partition table found in the boot image...)
            WriteZeroBytes(1);
            WriteLittleEndianUInt16(entry.SectorCount);
            WriteLittleEndianUInt32(dataSector);
            WriteZeroBytes(4);
        }

        void WriteBootRecord(uint sectorOfBootCatalog) {
            Debug.Assert(sectorOfBootRecord.HasValue, "Boot sector must be allocated before writing it");

            WriteVolumeDescriptorHeader(VolumeDescriptorType.BootRecord);
            WriteAString(elToritoSystemIdentifier, 64, context: "Boot System Identifier", padding: 0);
            WriteLittleEndianUInt32(sectorOfBootCatalog);
        }
        #endregion

        #region Path Table
        void WritePathTable(Volume volume, bool bigEndian) {
            if (volume == null)
                throw new ArgumentNullException("volume");
            if (volume.RootDirectory == null)
                throw ArgumentException("volume", "The given volume ({0}) does not have a root directory and therefore it is not possible to write the path table for it", volume.VolumeIdentifier ?? "Unidentified");
         
            // As with the rest of ISO-9660, path table numbers start at 1.
            WritePathTable(volume.RootDirectory, 1, 1, bigEndian);
        }

        ushort WritePathTable(Directory directory, ushort recordNumber, ushort parentRecordNumber, bool bigEndian) {
            if (directory == null)
                throw new ArgumentNullException("directory");

            ushort myRecordNumber = recordNumber;

            WritePathTableRecord(directory, recordNumber, parentRecordNumber, bigEndian);
            checked {
                recordNumber++;
            }

            // TODO Order the path table records according to ISO-9660 6.9.1
            if (directory.Entries != null)
                foreach (var dir in directory.Entries.OfType<Directory>())
                    recordNumber = WritePathTable(dir, recordNumber, myRecordNumber, bigEndian);

            return recordNumber;
        }

        void WritePathTableRecord(Directory directory, ushort recordNumber, ushort parentRecordNumber, bool bigEndian) {
            // If it's the root directory, we should use the special name for it.
            var directoryIdentifier = recordNumber == parentRecordNumber ? new byte[1] : directory.MappedIdentifier;
            if (directoryIdentifier.Length > byte.MaxValue)
                throw ArgumentException("directory", "Cannot write the path table entry for this directory ({0}) because its length ({1}) is greater than the maximum allowed value of ({2})", directory.Name ?? "Unidentified", directoryIdentifier.Length, byte.MaxValue);

            WriteByte((byte)directoryIdentifier.Length);
            WriteZeroBytes(1); // Extended attribute record length

            var dlm = GetDirectoryLocationMap(directory);
            if (bigEndian) {
                WriteBigEndianUInt32(LbaFromSector(dlm.ExtentSector));
                WriteBigEndianUInt16(parentRecordNumber);
            } else {
                WriteLittleEndianUInt32(LbaFromSector(dlm.ExtentSector));
                WriteLittleEndianUInt16(parentRecordNumber);
            }

            Write(directoryIdentifier);
            if (directoryIdentifier.Length % 2 == 1)
                WriteZeroBytes(1); // Padding
        }
        #endregion

        void WriteVolumeDescriptorSetTerminator() {
            WriteVolumeDescriptorHeader(VolumeDescriptorType.Terminator);
            SeekToNextSector();
        }
        #endregion

        #region Writing helpers
        readonly ASCIIEncoding ascii = new ASCIIEncoding();

        // Date/Time values in directory records are encoded differently (more compactly).
        void WriteDateTimeForVolumeDescriptor(DateTime? dateTime) {
            if (!dateTime.HasValue) {
                WriteDString(new string('0', 16), 17, "Date and Time", 0);
                return;
            }

            var formatted = dateTime.Value.ToString("yyyyMMddhhmmssff", CultureInfo.InvariantCulture);

            // XXX Assumes that GMT will always be the same as UTC
            var inUtc = TimeZoneInfo.ConvertTimeToUtc(dateTime.Value);
            var gmtOffset = checked((sbyte)((dateTime.Value - inUtc).Minutes / 15)); // Signed 8-bit value, -1 = -15 minutes from UTC, +4 = 1 hour ahead of UTC

            WriteDString(formatted, 16, "Date and Time");
            WriteSignedByte(gmtOffset);
        }

        void WriteDateTimeForDirectoryRecord(DateTime dateTime) {
            // TODO Might be corrupted if year can't fit in byte
            WriteByte((byte)(dateTime.Year - 1900));
            WriteByte((byte)dateTime.Month);
            WriteByte((byte)dateTime.Day);
            WriteByte((byte)dateTime.Hour);
            WriteByte((byte)dateTime.Minute);
            WriteByte((byte)dateTime.Second);

            var inUtc = TimeZoneInfo.ConvertTimeToUtc(dateTime);
            var gmtOffset = checked((sbyte)((dateTime - inUtc).Minutes / 15)); // Signed 8-bit value, -1 = -15 minutes from UTC, +4 = 1 hour ahead of UTC
            WriteSignedByte(gmtOffset);
        }

        byte[] EncodeAString(string str, int length, string context = "current") {
            return EncodeAorDString(str, length, aCharacters, context);
        }

        byte[] EncodeDString(string str, int length, string context = "current") {
            return EncodeAorDString(str, length, dCharacters, context);
        }

        void WriteAString(string str, int length, string context = "current", byte padding = 0x20) {
            WriteAorDString(str, length, aCharacters, context, padding);
        }

        void WriteDString(string str, int length, string context = "current", byte padding = 0x20) {
            WriteAorDString(str, length, dCharacters, context, padding);
        }

        // TODO Check that file is not ".", version number between 1 and 32767
        void WriteFileIdentifier(string str, int length, string context = "current", byte padding = 0x20) {
            WriteAorDString(str, length, dCharactersAndSeparators, context, padding);
        }

        void WriteAorDString(string str, int length, string allowedCharacters, string context = "current", byte padding = 0x20) {
            byte[] data = EncodeAorDString(str, length, allowedCharacters, context);
            Write(data);

            while (data.Length < length--)
                Write(padding);
        }

        byte[] EncodeAorDString(string str, int length, string allowedCharacters, string context = "current") {
            if (string.IsNullOrEmpty(str))
                return new byte[0];

            foreach (char c in str)
                if (allowedCharacters.IndexOf(c) < 0)
                    throw ArgumentException("str", "The character '{0}' is not allowed in the {1} context. The following characters are allowed: {2})", c, context, allowedCharacters);

            byte[] data = ascii.GetBytes(str);
            if (data.Length > length)
                throw ArgumentException("str", "The maximum length allowed in the {0} context is {1} bytes, but {2} bytes were given", context, length, data.Length);

            return data;
        }

        void WriteZeroBytes(int count) {
            Write(new byte[count]);
        }

        void Write(byte b) {
            stream.WriteByte(b);
        }

        void WriteByte(byte b) {
            stream.WriteByte(b);
        }

        void WriteSignedByte(sbyte b) {
            stream.WriteByte((byte)b);
        }

        void Write(byte[] data) {
            stream.Write(data, 0, data.Length);
        }

        void WriteLittleEndianUInt16(UInt16 value) {
            stream.WriteByte((byte)value);
            stream.WriteByte((byte)(value >> 8));
        }

        void WriteBigEndianUInt16(UInt16 value) {
            stream.WriteByte((byte)(value >> 8));
            stream.WriteByte((byte)value);
        }

        void WriteBothEndianUInt16(UInt16 value) {
            WriteLittleEndianUInt16(value);
            WriteBigEndianUInt16(value);
        }

        void WriteLittleEndianUInt32(UInt32 value) {
            stream.WriteByte((byte)value);
            stream.WriteByte((byte)(value >> 8));
            stream.WriteByte((byte)(value >> 16));
            stream.WriteByte((byte)(value >> 24));
        }

        void WriteBigEndianUInt32(UInt32 value) {
            stream.WriteByte((byte)(value >> 24));
            stream.WriteByte((byte)(value >> 16));
            stream.WriteByte((byte)(value >> 8));
            stream.WriteByte((byte)value);
        }

        void WriteBothEndianUInt32(UInt32 value) {
            WriteLittleEndianUInt32(value);
            WriteBigEndianUInt32(value);
        }
        #endregion

        #region Exception Code
        static ArgumentException ArgumentException(string argumentName, string messageFormat, params object[] inserts) {
            return new ArgumentException(string.Format(messageFormat, inserts), argumentName);
        }

        static InvalidOperationException InvalidOperationException(string messageFormat, params object[] inserts) {
            return new InvalidOperationException(string.Format(messageFormat, inserts));
        }
        #endregion
    }
}