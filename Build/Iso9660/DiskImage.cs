using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
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
        LimitDirectories = 1,
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
        /// <remarks>Could go into an infinite loop</remarks>
        public Volume(string path, bool useDirectoryDates, bool useUtcDates) : this() {
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

            RootDirectory = new Directory(dir);
        }
    }

    public class Directory {
        public DateTime? RecordingDateTime { get; set; }

        public bool UserVisible { get; set; }
        public bool AssociatedFile { get; set; }
        public bool Record { get; set; }
        public bool Protection { get; set; }
        public bool MultiExtent { get; set; }

        public string Name { get; set; }

        public List<Directory> Directories { get; set; }
        public List<File> Files { get; set; }

        public Directory() {
            Directories = new List<Directory>();
            Files = new List<File>();
        }

        public Directory(IO.DirectoryInfo dir) : this() {
            Name = dir.Name;
            UserVisible = true;

            foreach(var dirInfo in dir.EnumerateDirectories())
                Directories.Add(new Directory(dirInfo));

            foreach(var fileInfo in dir.EnumerateFiles())
                Files.Add(new File(fileInfo));
        }
    }

    public class File {
        public DateTime? RecordingDateTime { get; set; }

        public bool UserVisible { get; set; }
        public bool AssociatedFile { get; set; }
        public bool Record { get; set; }
        public bool Protection { get; set; }
        public bool MultiExtent { get; set; }

        public string Name { get; set; }
        public string FileSystemPath { get; set; }

        public File() {
        }

        public File(IO.FileInfo fileInfo) : this() {
            Name = fileInfo.Name;
            FileSystemPath = fileInfo.FullName;
            UserVisible = true;
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
            CompatibilityFlags compatibilityFlags = CompatibilityFlags.Strict, 
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
        readonly int bytesInLogicalBlock = 512;

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

            public VolumeLocationMap(uint sectorOfDescriptor) {
                SectorOfVolumeDescriptor = sectorOfDescriptor;
            }
        }

        /// <summary>
        /// Directory extents are located at the start of the first logical block in a logical sector.
        /// </summary>
        class DirectoryLocationMap {
            public bool Written { get; set; }
            public uint SectorOfDirectoryDescriptor { get; private set; }

            public DirectoryLocationMap(uint sectorOfDescriptor) {
                SectorOfDirectoryDescriptor = sectorOfDescriptor;
            }
        }

        class FileLocationMap {
            public bool Written { get; set; }
            public uint SectorOfFileDescriptor { get; private set; }

            public FileLocationMap(uint sectorOfDescriptor) {
                SectorOfFileDescriptor = sectorOfDescriptor;
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

        void AllocateDirectoryDescriptor(Directory directory) {
            if (directory == null)
                throw new ArgumentNullException("directory");
            if (directories.ContainsKey(directory))
                throw InvalidOperationException("A directory descriptor for this directory ({0}) has already been allocated", directory.Name ?? "null");

            directories.Add(directory, new DirectoryLocationMap(GetCurrentLogicalSector()));
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

            var vlm = GetVolumeLocationMap(volume);

            WriteDirectory(volume.RootDirectory, null);

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
            SeekToSector(vlm.SectorOfVolumeDescriptor);

            WriteVolumeDescriptorHeader(isPrimary ? VolumeDescriptorType.Primary : VolumeDescriptorType.Supplementary);

            WriteByte(0);
            WriteAString(volume.SystemIdentifier, 32, "System Identifier");
            WriteDString(volume.VolumeIdentifier, 32, "Volume Identifier");
            WriteZeroBytes(8);

            // TODO Volume Space Size
            WriteZeroBytes(8);

            WriteZeroBytes(32);

            WriteBothEndianUInt16(volume.VolumeSetSize);
            WriteBothEndianUInt16(volume.VolumeSequenceNumber);
            
            Debug.Assert(volume.LogicalBlockSize == bytesInLogicalBlock, "Logical block size cannot currently be changed");
            WriteBothEndianUInt16(volume.LogicalBlockSize);

            // TODO Path Table Size
            WriteZeroBytes(8);

            // TODO Type L Path Table Location
            WriteZeroBytes(4);

            // TODO Optional Type L Path Table Location
            WriteZeroBytes(4);

            // TODO Type M Path Table Location
            WriteZeroBytes(4);

            // TODO Optional Type M Path Table Location
            WriteZeroBytes(4);

            // TODO Directory Record For Root Directory
            WriteZeroBytes(34);


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

        #region Directory Record
        /// <summary>
        /// Writes all the directory records for a directory and its files and subdirectories.
        /// </summary>
        /// <param name="directory">The directory to write.</param>
        /// <param name="sectorOfParentDirectory">The sector of the parent directory, or 
        /// <c>null</c> if the directory is the root directory.</param>
        void WriteDirectory(Directory directory, uint? sectorOfParentDirectory) {
            if (directory == null)
                throw new ArgumentNullException("directory");

            uint sector = GetCurrentLogicalSector();
            
            // We don't yet know the data length of this directory, however we can go back
            // and set it once we do know it... we just need to know where to write the 
            // data length to once we're finished

            const byte fileIdentifierOfCurrentDirectory = 0;
            const byte fileIdentifierOfParentDirectory = 1;

            // Absolute Byte position (BP) of the first directory record in the disk image.
            long bpOfFirstDirectoryRecord = stream.Position;

            // Write directory record for the current directory.
            // TODO Get flags/time zone from user/settings
            WriteDirectoryRecord(
                new byte[] { fileIdentifierOfCurrentDirectory }, 
                sector, 
                0,
                DateTime.Now,
                FileFlags.Directory,
                1);

            // Absolute Byte position (BP) of the second directory record in the disk image.
            long bpOfSecondDirectoryRecord = stream.Position;

            // Write parent directory record. If the sector of the parent directory isn't 
            // specified then this directory is the root directory.
            WriteDirectoryRecord(
                new byte[] { fileIdentifierOfParentDirectory }, 
                sectorOfParentDirectory ?? sector, 
                0,
                DateTime.Now,
                FileFlags.Directory, 
                1);

            // Relative byte position (RBP) of the data length field in the 
            // directory record.
            const int rbpOfDataLength = 10;

            // TODO Compute data length...
            uint dataLength = checked((uint)(stream.Position - bpOfFirstDirectoryRecord));

            // Write the data length to the first two records
            PreservingLocation(() => {
                stream.Position = bpOfFirstDirectoryRecord + rbpOfDataLength;
                WriteBothEndianUInt32(dataLength);
                stream.Position = bpOfSecondDirectoryRecord + rbpOfDataLength;
                WriteBothEndianUInt32(dataLength);
            });
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
        void WriteDirectoryRecord(byte[] fileIdentifier, uint sectorOfExtent, uint dataLength, DateTime recordingDateTime, FileFlags flags, int volumeSequenceNumber) {
            if (fileIdentifier == null)
                throw new ArgumentNullException("fileIdentifier");
            if (fileIdentifier.Length == 0)
                throw ArgumentException("fileIdentifier", "The file or directory identifier must contain at least one byte");

            const int bytesInBaseDirectoryRecord = 33;

            int recordLength = fileIdentifier.Length + bytesInBaseDirectoryRecord;
            if (recordLength % 2 == 1)
                recordLength++; // Pad to an even length

            if (recordLength > byte.MaxValue)
                throw ArgumentException("directory", "The file or directory name (\"{0}\") is too long to fit in the directory record. The maximum length allowed by the ISO-9660 specification, in any compatibility level, is 31 bytes.", ascii.GetString(fileIdentifier));
        }

        byte[] EncodeFileName(string name) {
            if (compatibilityLevel == CompatibilityLevel.Level1) {
                // TODO Ensure proper 8.3 compliance or just 8-compliance for directories

                var sb = new StringBuilder(name);
                foreach (char c in name)
                    if (dCharactersAndSeparators.IndexOf(c) >= 0)
                        sb.Append(c);
                name = sb.ToString();
            }

            // TODO Ensure length does not exceed 31 in Compatibility Level 2 and above

            // TODO Strip out characters that cannot be represented in ISO-646
            return ascii.GetBytes(name);
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

            int sectorsOfData = data.Length / bytesInSector;
            if (sectorsOfData < 1)
                sectorsOfData = 1;

            stream.Write(data, 0, data.Length);

            sector = checked((uint)(sectorsOfData + sector));
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

    public class DiskImageWriter2 : IDisposable {
        readonly string path;
        readonly IO.Stream stream;

        readonly Mode mode;
        readonly CompatibilityLevel compatibilityLevel;
        readonly CompatibilityFlags compatibilityFlags;
        readonly Extensions extensions;

        const int bytesInSector = 2048;
        const int sectorsInSystemArea = 16;
        readonly int bytesInLogicalBlock = 512;

        const int bytesInVolumeDescriptorHeader = 7;
        const int bytesInVolumeDescriptorData = bytesInSector - bytesInVolumeDescriptorHeader;

        const string dCharacters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        const string dCharactersAndSeparators = ".0123456789;ABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        const string aCharacters = " !=%&'()*+,-./0123456789:;<=>?ABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        const byte fileNameExtensionSeparator = 0x2E; // .
        const byte fileNameVersionSeparator = 0x3B; // ;

        public DiskImageWriter2(
            string path, 
            Mode mode = Mode.Mode1, 
            CompatibilityLevel compatibilityLevel = CompatibilityLevel.Level1, 
            CompatibilityFlags compatibilityFlags = CompatibilityFlags.Strict, 
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

        private void ValidateOptions() {
            if (mode != Mode.Mode1)
                throw new NotSupportedException("Only ISO 9660 Mode 1 images are supported");
            if ((extensions & Extensions.Apple) == Extensions.Apple)
                throw new NotSupportedException("The Apple ISO 9660 extensions are not supported");
            if ((extensions & Extensions.Udf) == Extensions.Apple)
                throw new NotSupportedException("UDF is not supported");
        }

        public string Path {
            get { return path; }
        }

        public string MediaType {
            get { return "application/x-iso9660-image"; }
        }

        #region Writing
        enum State {
            Uninitialized,
            WritingVolumeDescriptors,
            WhateverComesNext
        }
        State state = State.Uninitialized;
        bool hasPrimaryVolumeDescriptor = false;
        readonly ASCIIEncoding ascii = new ASCIIEncoding();

        /// <summary>
        /// Initializes the System Area of the disk image (the first 16 sectors).
        /// </summary>
        public void Begin() {
            if (state != State.Uninitialized)
                throw new InvalidOperationException("Cannot call Begin() twice");

            Debug.Assert(stream.Position == 0, "stream.Position == 0");

            // This will set the bytes in the system area to zero (unless the code is running on 
            // Windows 98 or earlier, but if you managed to get .NET 4.5 running on Windows 98,
            // I'm sure you can change this code too).
            stream.Seek(bytesInSector * sectorsInSystemArea, IO.SeekOrigin.Begin);
            state = State.WritingVolumeDescriptors;
        }

        private void CheckBegun([CallerMemberName] string method = "Unknown") {
            if (state == State.Uninitialized)
                throw new InvalidOperationException(string.Format("You must call Begin() before calling {0}()", method));
        }

        private long WriteVolumeDescriptorHeader(VolumeDescriptorType type, byte version = 1) {
            Debug.Assert(stream.Position % bytesInSector == 0, "Volume descriptor must be placed at the start of a logical sector");
            if (stream.Position == bytesInSector * sectorsInSystemArea) {
                if (type == VolumeDescriptorType.Terminator)
                    throw new InvalidOperationException("You must at least write a primary volume descriptor");
                if (type != VolumeDescriptorType.Primary)
                    throw new InvalidOperationException("The first volume descriptor must be a primary volume descriptor");
            }

            long nextSectorStart = stream.Position + bytesInSector;
            
            Write((byte)type);
            WriteDString("CD001", 5, "Standard Identifier");
            Write(version);

            return nextSectorStart;
        }

        public void WriteBootRecord(string bootSystemIdentifier, string bootIdentifier, byte[] bootData) {
            if (bootSystemIdentifier == null)
                throw new ArgumentNullException("bootSystemIdentifier");
            if (bootIdentifier == null)
                throw new ArgumentNullException("bootIdentifier");

            var nextPos = WriteVolumeDescriptorHeader(VolumeDescriptorType.BootRecord);

            const int bootIdLength = 32;
            const int allowedBytes = bytesInVolumeDescriptorData - bootIdLength * 2;
            if (bootData.Length > allowedBytes)
                throw ArgumentException("bootData", "The boot sector data was too long ({0} bytes). The maximum size allowed is {1} bytes.", bootData.Length, allowedBytes);

            WriteAString(bootSystemIdentifier, bootIdLength, "Boot System Identifier");
            WriteAString(bootIdentifier, bootIdLength, "Boot Identifier");
            Write(bootData);

            FinishWritingVolumeDescriptor(nextPos);
        }

        public void WriteElToritoBootRecord(uint bootCatalogLba) {
            var nextPos = WriteVolumeDescriptorHeader(VolumeDescriptorType.BootRecord);

            WriteAString("EL TORITO SPECIFICATION", 64, context: "Boot System Identifier", padding: 0);

            WriteLittleEndianUInt32(bootCatalogLba);

            FinishWritingVolumeDescriptor(nextPos);
        }

        public void WritePrimaryVolumeDescriptor(string systemIdentifier, string volumeIdentifier) {
            if (systemIdentifier == null)
                throw new ArgumentNullException("systemIdentifier");
            if (volumeIdentifier == null)
                throw new ArgumentNullException("volumeIdentifier");

            if (hasPrimaryVolumeDescriptor)
                throw new InvalidOperationException("You may only write one primary volume descriptor; consider using a supplementary volume descriptor instead");

            var nextPos = WriteVolumeDescriptorHeader(VolumeDescriptorType.Primary);

            WriteByte(0);
            WriteAString(systemIdentifier, 32, "System Identifier");
            WriteDString(volumeIdentifier, 32, "Volume Identifier");
            WriteZeroBytes(8);
            // TODO More fields

            // TODO Volume Space Size
            WriteZeroBytes(8);

            WriteZeroBytes(32);

            // TODO Volume Set Size
            WriteByte(1);
            WriteByte(0);
            WriteByte(0);
            WriteByte(1);

            // TODO Volume Sequence Number
            WriteByte(1);
            WriteByte(0);
            WriteByte(0);
            WriteByte(1);

            // TODO Logical Block Size
            WriteByte(0);
            WriteByte(8);
            WriteByte(8);
            WriteByte(0);

            // TODO Path Table Size
            WriteZeroBytes(8);

            // TODO Type L Path Table Location
            WriteZeroBytes(4);

            // TODO Optional Type L Path Table Location
            WriteZeroBytes(4);

            // TODO Type M Path Table Location
            WriteZeroBytes(4);

            // TODO Optional Type M Path Table Location
            WriteZeroBytes(4);

            // TODO Directory Record For Root Directory
            WriteZeroBytes(34);

            WriteDString("BOMAGOS_INSTALL_DISK", 128, "Volume Set Identifier");
            WriteAString("BOMAG", 128, "Publisher Identifier");
            WriteAString("", 128, "Data Preparer Identifier");
            WriteAString("", 128, "Application Identifier");
            WriteFileIdentifier("", 37, "Copyright File Identifier"); // TODO Check 8.3
            WriteFileIdentifier("", 37, "Abstract File Identifier"); // TODO Check 8.3
            WriteFileIdentifier("", 37, "Bibliographic File Identifier"); // TODO Check 8.3

            WriteAString("0000000000000000", 17, "Creation Date and Time", 0);
            WriteAString("0000000000000000", 17, "Modification Date and Time", 0);
            WriteAString("0000000000000000", 17, "Expiration Date and Time", 0);
            WriteAString("0000000000000000", 17, "Effective Date and Time", 0);

            // TODO File Structure Version
            WriteByte(1);

            WriteByte(0);

            hasPrimaryVolumeDescriptor = true;
            FinishWritingVolumeDescriptor(nextPos);
        }

        public void WriteVolumeDescriptorTerminator() {
            if (state == State.Uninitialized)
                throw new InvalidOperationException("You must initialize the disk image before writing the volume descriptor terminator");
            if (state != State.WritingVolumeDescriptors)
                throw new InvalidOperationException("You cannot write another volume descriptor terminator after the volume descriptor terminator has been written");

            FinishWritingVolumeDescriptor(WriteVolumeDescriptorHeader(VolumeDescriptorType.Terminator));
            state = State.WhateverComesNext;
        }

        private void CheckStateForWritingVolumeDescriptor() {
            if (state == State.Uninitialized)
                throw new InvalidOperationException("You must initialize the disk image before writing volume descriptors");
            if (state != State.WritingVolumeDescriptors)
                throw new InvalidOperationException("You cannot write volume descriptors after the volume descriptor terminator has been written");
        }

        private void FinishWritingVolumeDescriptor(long nextSectorStart) {
            Debug.Assert(stream.Position < nextSectorStart, "Volume descriptor data overran the sector");
            stream.Seek(nextSectorStart, IO.SeekOrigin.Begin);
        }

        const int bytesInElToritoBootCatalogEntry = 32;
        public void WriteElToritoBootCatalogValidationEntry(ElToritoPlatformId platformId, string idString) {
            if (idString == null)
                throw new ArgumentNullException("idString");

            Debug.Assert(stream.Position % bytesInSector == 0, "Boot catalog must be placed at the start of a logical sector");
            // Write validation entry
            WriteByte(1);
            WriteByte((byte)platformId);
            WriteZeroBytes(2);
            WriteAString(idString, 24, "ID String");
            WriteZeroBytes(2); // The checksum will go here
            WriteByte(0x55);
            WriteByte(0xAA);

            // Read back the bytes we have written
            stream.Seek(-bytesInElToritoBootCatalogEntry, IO.SeekOrigin.Current);

            var validationEntry = new byte[bytesInElToritoBootCatalogEntry];
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

        public void WriteElToritoBootCatalogDefaultEntry(ElToritoMediaType mediaType, uint bootSectorCodeLba, ushort sectorCount, ushort loadSegment = 0) {
            WriteByte(0x88); // Mark as bootable
            WriteByte((byte)mediaType);
            WriteLittleEndianUInt16(loadSegment); // Will be 7C0 if set to zero
            WriteByte(0); // System type (spec says this should be byte 5 of the partition table found in the boot image...)
            WriteZeroBytes(1);
            WriteLittleEndianUInt16(sectorCount);
            WriteLittleEndianUInt32(bootSectorCodeLba);
            WriteZeroBytes(4);
        }

        // If no data has been written to the boot catalog it will still go forward a sector.
        public void FinishWritingElToritoBootCatalog() {
            long offset = stream.Position % bytesInSector;
            stream.Seek(bytesInSector - offset, IO.SeekOrigin.Current);
        }

        public void WriteBootSector(uint sectorCount, byte[] data) {
            long startPos = stream.Position;
            Write(data);
            stream.Seek(startPos + sectorCount * bytesInSector, IO.SeekOrigin.Begin);
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

        void Write(byte[] data) {
            stream.Write(data, 0, data.Length);
        }

        void WriteLittleEndianUInt16(UInt16 value) {
            stream.WriteByte((byte)value);
            stream.WriteByte((byte)(value >> 8));
        }

        void WriteLittleEndianUInt32(UInt32 value) {
            stream.WriteByte((byte)value);
            stream.WriteByte((byte)(value >> 8));
            stream.WriteByte((byte)(value >> 16));
            stream.WriteByte((byte)(value >> 24));
        }
        #endregion

        #region Helpers
        ArgumentException ArgumentException(string argumentName, string messageFormat, params object[] inserts) {
            return new ArgumentException(string.Format(messageFormat, inserts), argumentName);
        }
        #endregion

        public void Dispose() {
            stream.Close();
        }
    }
}