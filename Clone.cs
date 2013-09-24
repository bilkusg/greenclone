// Copyright (C) Gary M. Bilkus 2013. All rights reserved
/*  This file is part of GreenClone.

    GreenClone is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    GreemClone is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with GreenClone.  If not, see <http://www.gnu.org/licenses/>.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Win32IF;
using VSS;

namespace Clone
{
    
    class MyBackup : Backup
    {
        public MyBackup(String ifile, String ofile): base(ifile,ofile)
        {
        }
        protected override  Boolean exclude(String fromFilename)
        {
            return base.exclude(fromFilename);
        }
    }
    class Clone
    {
        private const String version = "Greenwheel clone version D907. Copyright (c) Gary M. Bilkus";
        [MTAThread]
        static void Main(string[] args)
        {
            Clone c = new Clone();
            c.run(args);
        }

        VssHelper vssHelper = null;
        List<String> excludePaths = new List<String>();
        Boolean excludingPaths = false;

        public void usage()
        {
            Console.WriteLine("Usage: Clone frompath topath [ options ]");
            Console.WriteLine(" Note each option must be separated by whitespace from the next");
            Console.WriteLine("/M - only copy reparse points and directories");
            Console.WriteLine("/NP - do not copy permissions as well as file data");
            Console.WriteLine("/K - create separate files with permission information and alternate streams");
            Console.WriteLine("/W - overwrite existing files where necessary");
            Console.WriteLine("/H - use hard links");
            Console.WriteLine("/Q - quiet - only error messages");
            Console.WriteLine("/V - verbose - report file progress");
            Console.WriteLine("/XP path - exclude files matching path within the hierarchy. Can be repeated");
            Console.WriteLine("/XL filename - Read exclude list from contents of file");
            Console.WriteLine("/SHADOW - Create a shadow copy for the source and read from that");
            Console.WriteLine("/QUICKVSS - Create a shadow copy without registering all writers. Faster than SHADOW and works when debugging");
        }

        public void processBatchExcludeFile(String filename)
        {
            String line;
            using (System.IO.StreamReader file = new System.IO.StreamReader(filename))
            {
                while ( (line = file.ReadLine()) != null)
                {
                    excludePaths.Add(line);
                }
            }

        }
        public void run(string[] args)
        {
            Console.WriteLine(version);
            String fromPath = "";
            String toPath = "";
            int nPaths = 0;
            int reportVerbosity = 3;
            // coarse grained command line options
            Boolean bkfile = false, reparseOnly = false, overwrite = false, useHardLinks = false, copyPermissions = false;
            Boolean useVss = false; Boolean useFullVss = true;
            Boolean waitingForExcludePath = false;
            Boolean waitingForBatchFilePath = false;

            foreach (String arg in args)
            {
                if (waitingForExcludePath)
                {
                    excludePaths.Add(arg);
                    waitingForExcludePath = false;
                    continue;
                }
                if (waitingForBatchFilePath)
                {
                    processBatchExcludeFile(arg);
                    waitingForBatchFilePath = false;
                    continue;
                }
                if (arg.StartsWith("/") || arg.StartsWith("-"))
                {
                    // this is an argument
                    switch (arg.Substring(1).ToUpper())
                    {
                        case "?":
                        case "HELP": usage(); return;
                        case "XL":
                            waitingForBatchFilePath = true;
                            break;
                        case "W":
                        case "MIR": overwrite = true; break;
                        case "NP":
                        case "NOPERMS": copyPermissions = true; break;
                        case "K":
                        case "BKFD": bkfile = true; break;
                        case "H":
                        case "HARDLINK":
                            useHardLinks = true; break;
                        case "Q":
                        case "QUIET":
                            reportVerbosity = 1; break;
                        case "V":
                        case "VERBOSE":
                            reportVerbosity = 5; break;
                        case "VV":
                        case "VERYVERBOSE":
                            reportVerbosity = 8; break;
                        case "XP":
                            waitingForExcludePath = true;
                            excludingPaths = true;
                            break;
                        case "SHADOW":
                            useVss = true; break;
                        case "QUICKVSS":
                            useVss = true;
                            useFullVss = false;
                            break;
                        default:
                            {
                                Console.Write("FAILED: Unrecognised option {0} use /? for help", arg);
                                return;
                            }
                    }
                }
                else if (nPaths == 0)
                {
                    fromPath = arg;
                    nPaths++;
                }
                else if (nPaths == 1)
                {
                    toPath = arg;
                    nPaths++;
                }
                else
                {
                    usage();

                    return;
                }
            };
            if (nPaths != 2)
            {
                usage();
                return;
            }
            Console.WriteLine("From:{0} -> {1}", fromPath, toPath);

            try
            {
                Win32IF.Privileges.obtainPrivileges();
                if (useVss)
                {
                    vssHelper = new VssHelper();
                    Console.WriteLine("VSS :INIT");
                    
                    String atDrive;
                    String afterDrive;
                    atDrive = fromPath;

                    if (atDrive.StartsWith(@"\\?\"))
                    {
                        
                        atDrive = atDrive.Substring(4);
                        if (atDrive.StartsWith("UNC"))
                        {
                            Console.WriteLine("Cannot use VSS with a network drive");
                            return;
                        }
                    }
                    if (atDrive.StartsWith(@"\\"))
                    {
                        Console.WriteLine("Cannot use VSS with a network drive");
                        return;
                    }

                    if (atDrive.ElementAt<Char>(1) != ':')
                    {
                        Console.WriteLine("To use VSS you must explicitly specify the drive containing the directory to be copied");
                        return;
                    }

                    afterDrive = atDrive.Substring(2);
                    atDrive = atDrive.Substring(0, 2) + @"\";


                    String[] drives = { atDrive };
                    vssHelper.CreateShadowsForDrives(drives,useFullVss);
                    String driveShadow = (String)vssHelper.pathToShadow[atDrive];
                    if (driveShadow == null)
                    {
                        Console.WriteLine("VSS creation failed - unable to continue");
                        return;
                    }
                    fromPath = driveShadow + afterDrive;
                    Console.WriteLine("VSS :{0}", fromPath);

                }
              
                Backup b = new MyBackup(fromPath, toPath);


                // The backup class has lots of fine-grained controls which we can set programmatically
                // This particular front end program only controls the most common options ( based on my assumptions about how it will be used 
                // Feel free to add your own options and set these flags accordingly
                b.recursive = true; // if false, only the named file will be copied ( or an empty directory if that's what it is ) mainly useful for testing.

                // these first options determine what kind of file we are interested in
                b.cloneDir = true; // if false, subdirectories will be ignored too.
                b.cloneFile = !reparseOnly; // if false we ignore normal files. This can be useful if we just want to copy reparse points
                b.cloneReparse = true; // if false we ignore reparse points and just copy normal files. This is seldom useful


                b.createBkf = bkfile;  // if true, we create a separate .bkfd file to contain information such as AFS and permissions which may be lost if the destination isn't NTFS
                // these options only apply if createBkf is true and should probably be left as is
                // if we create a bkfd file we always store the original file attributes and permissions otherwise there's no point in having it
                // but these allow us to decide whether to store some or all of the data in the file to the bkfs file.
                b.createBkfAFS = true; // we send AFS to the bkfs file as well as to the main file ( if possible )
                b.createBkfData = false; // we send the actual file contents to the bkfs file. We're unlikely to use this


                b.restorePermissions = copyPermissions && !bkfile; // if true, we restore permissions from the bkfd file if it exists, or clone them from the source otherwise
                // note that if we are writing a .bkfd file, we never restore permissions to the file itself.
                b.restoreAFS = true; // if we are able to, we restore AFS to the destination from the .bkfd if it exists, otherwise we clone them from the source.
                b.restoreAttributes = true;  // if we are able to, we restore file attributes ( system, hidden, readonly etc ) from the .bkfd file if it exists or we clone from source
                b.restoreFileContents = true; // if set to false, the main stream in the source file will be ignored. Seldom useful.
                b.restoreHardLinks = true;  // if set to true, any hardlinks between files which are part of the same backup will be restored as hard links on the destination.
                b.restoreTimes = true; // otherwise all destination files show as created at the time of the copy
                b.useHardLinks = useHardLinks; // if true, attempt to create destination files by hardlinking from the source. Useful in some scenarios
                b.overwriteDir = overwrite; // if true, allow a directory in the destination to be deleted completely and replaced with a normal file or reparse point
                b.overwriteFile = overwrite; // if true, allow changed files to overwrite in the destination
                b.overwriteReparse = overwrite; // if true, allow changed reparse points to overwrite in the destination
                b.removeExtra = overwrite; //  automatically remove from the destination any files not found in the source

                b.cloneAlsoUsesBkf = true;
                Backup.reportVerbosity = reportVerbosity;
                if (excludingPaths)
                {
                    b.excludeList = excludePaths;
                }

                b.doit();
                Console.WriteLine("IN:Dirs:{0} Files:{1} Special:{2} Ignored:{3}", b.nDirs, b.nFiles, b.nSpecial, b.nIgnored);
                Console.WriteLine("OUT:Same:{0} Overwritten:{1} Deleted:{2} IntHLinked:{3}", b.nSame, b.nOverwritten, b.nDeleted,b.nInternalHardLinked);
                if (b.nFailed > 0)
                {
                    Console.WriteLine("FAILED:{0}", b.nFailed);
                }
                else
                {
                    Console.WriteLine("SUCCEEDED");
                }
            }
            finally
            {
                if (useVss && vssHelper != null)
                {
                    vssHelper.DeleteShadows();
                    Console.WriteLine("VSS :CLEAN");
                }
            }
        }
    }
}
