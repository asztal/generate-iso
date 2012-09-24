using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Bomag.Build.Iso9660;
using Directory = System.IO.Directory;
using DiskImage = Bomag.Build.Iso9660.DiskImage;
using File = System.IO.File;

namespace Bomag.Build {
    class Program {
        static void Main(string[] args) {
            try {

                if (args.Length != 3) {
                    Console.WriteLine(
                        "Usage: {0} path-to-content boot-sector output-file",
                        Path.GetFileNameWithoutExtension(Assembly.GetExecutingAssembly().Location));
                    return;
                }

                string contentPath = MakeAbsolutePath(args[0]);
                string bootSectorPath = MakeAbsolutePath(args[1]);
                string outputPath = MakeAbsolutePath(args[2]);

                if (!Directory.Exists(contentPath))
                    throw new DirectoryNotFoundException(string.Format("The given content path ({0}) does not exist or is not a directory.", contentPath));

                if (!File.Exists(bootSectorPath))
                    throw new FileNotFoundException(string.Format("The given boot sector path ({0}) does not exist or is not a file.", bootSectorPath));

                var diskImage = new DiskImage {
                    PrimaryVolume = new Volume(contentPath, true, false) {
                        VolumeIdentifier = "BOMAGOS",
                        SystemIdentifier = "X86",
                        VolumeSetIdentifier = "BOMAGOS",
                        DataPreparerIdentifier = "LEE HOUGHTON",
                        PublisherIdentifier = "LEE HOUGHTON"
                    },
                    BootCatalog = new BootCatalog {
                        IdString = "BOMAG",
                        PlatformId = ElToritoPlatformId.X86,
                        InitialEntry = new BootCatalogEntry {
                            Bootable = true,
                            MediaType = ElToritoMediaType.NoEmulation,
                            SectorCount = 1,
                            Data = File.ReadAllBytes(bootSectorPath)
                        }
                    }
                };

                using (var writer = new DiskImageWriter(outputPath)) {
                    writer.WriteDiskImage(diskImage);
                }
            } catch (Exception e) {
                Console.WriteLine("*** {1}: {0}", e.Message, e.GetType().Name);
            }
        }

        static string MakeAbsolutePath(string path) {
            return Path.IsPathRooted(path) ? path : Path.Combine(Directory.GetCurrentDirectory(), path);
        }
    }
}
