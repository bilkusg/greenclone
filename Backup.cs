// Copyright (C) Gary M. Bilkus 2013. All rights reserved
/*  This file is part of GreenClone.

    GreenClone is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    GreenClone is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with GreenClone.  If not, see <http://www.gnu.org/licenses/>.
*/

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Runtime.InteropServices;
using System.IO;
using System.ComponentModel;
using System.Collections;
using Win32IF;
using System.Diagnostics;
using System.Security.Cryptography;

public class DeletionHelper
{
    // if you want a job done properly.....
    // The methods in this class will delete a directory and all its contents by hook or crook
    private Hashtable emptyHashtable = new Hashtable();
    public virtual void reporter(string filename, bool isDir, bool isKept)
    {
        if (Backup.reportVerbosity <= 5) return;
        if (isKept)
        {
            //            Console.Error.WriteLine("                  :KEEP:{0}", filename);
            return;
        }
        if (isDir)
        {
            Console.Error.WriteLine("...DeletingDir:{0}", filename);
        }
        else
        {
            Console.Error.WriteLine("...DeletingFile:{0}", filename);
        }
    }
    public int DeletePathAndContentsRegardless(string foundFileName)
    {
        return DeletePathAndContentsUnless(foundFileName, emptyHashtable);
    }
    public int DeletePathAndContentsUnless(string foundFileName, Hashtable filesToKeep)
    {
        // this no nonsense method will do its damndest to delete a path regardless of what it is or what ( if a directory ) it contains.
        // The one thing we can't prevent is failure if one of the files in the path is locked by another running process. Apart from that, 
        // if you have backup rights and the path is legitimate, it should always work.....if not there's yet another edge case to worry about.
        int nDeleted = 0;
        FileAttributes attribs = W32File.GetFileAttributes(foundFileName);
        if (attribs == (FileAttributes)(-1))
        {
            return 0; // nothing to do
        }
        if (attribs.HasFlag(FileAttributes.Directory) && !attribs.HasFlag(FileAttributes.ReparsePoint))
        {
            int nd =
             DeleteDirectoryContentsRecursivelyUnless(foundFileName, filesToKeep);
            if (nd < 0) return nd;
            nDeleted += nd;
        }
        if (filesToKeep.ContainsKey(foundFileName)) // this file is one of ours, so hands off!
        {
            reporter(foundFileName, false, true);
            return nDeleted;
        }
        if (attribs.HasFlag(FileAttributes.ReadOnly))
        {
            W32File.SetFileAttributes(foundFileName, FileAttributes.Normal);
        }
        if (attribs.HasFlag(FileAttributes.Directory))
        {
            reporter(foundFileName, true, false);
            if (!W32File.RemoveDirectory(foundFileName))
            {
                return -1;
            };
            nDeleted++;
        }
        else
        {
            reporter(foundFileName, false, false);
            if (!W32File.DeleteFile(foundFileName))
            {
                return -1;
            };
            nDeleted++;
        }

        return nDeleted;
    }
    public int DeleteDirectoryContentsRecursively(string directoryPath)
    {
        return DeleteDirectoryContentsRecursivelyUnless(directoryPath, emptyHashtable);
    }
    public int DeleteDirectoryContentsRecursivelyUnless(string directoryPath, Hashtable filesToKeep)
    {
        int nDeleted = 0;
        FileFind.WIN32_FIND_DATA fdata = new FileFind.WIN32_FIND_DATA();
        IntPtr findhandle;
        findhandle = FileFind.FindFirstFile(directoryPath + @"\*", ref fdata);
        try
        {
            if (findhandle != W32File.INVALID_HANDLE_VALUE)
            {
                do
                {
                    if ((fdata.cFileName == @".") || (fdata.cFileName == @".."))
                    {
                        continue; // we don't want these
                    }
                    String foundFileName = directoryPath + @"\" + fdata.cFileName;
                    int nd =
                     DeletePathAndContentsUnless(foundFileName, filesToKeep);
                    if (nd < 0) return nd;
                    nDeleted += nd;
                }
                while (FileFind.FindNextFile(findhandle, ref fdata));

            };
            return nDeleted;
        }
        finally
        {
            FileFind.FindClose(findhandle);
        }
    }
};
class BackupReader
{
    static IntPtr pBuffer = Marshal.AllocHGlobal(1024 * 1034); // 1MB plus an extra 10K so we don't split a stream header;
    private bool readingBkFile = true;
    uint pBufferSize = 0;
    IntPtr fromFd = W32File.INVALID_HANDLE_VALUE;
    IntPtr fromBkFd = W32File.INVALID_HANDLE_VALUE;


    public W32File.WIN32_STREAM_ID streamHeader;
    public bool atNewStreamHeader = false;
    public bool atLastSegmentOfCurrentStream = false;
    public bool atEndOfFile = false; // this means we've actually finished delivering the entire file so there's nothing left to do
    public bool atStartOfFile = true;
    public IntPtr rcontext = IntPtr.Zero;
    public bool failed = false;
    public uint pBufferPos = 0;
    public bool entireFileRead = false; // this means the last read was less than asked, so we've no more data left to read  but the buffer may not all have been processed
    public int currentStreamNameSize;
    public string currentStreamName;
    private UInt64 currentStreamDataPos;
    public IntPtr streamDataSegmentStart;
    public uint streamDataSegmentSize;

    /*
     * The BackupReader class is a helper class for the main cloner
     * It is responsible for reading the data about a file in the form provided by the BackupRead function under Windows, which is the only reliable way of getting full
     * information about a file, including permissions, alternate file streams and permissions
     * It gets the data either by calling BackupRead on a file, or by reading a normal file containing the result of a previous BackupRead
     * Either way, it parses the data into the sequence of streams defined by BackupRead, so as to allow intelligent parsing and information provision.
     * Each call either returns true and some data, or false and either an error or indication that the file has been read.
     * Each call will either return the header for a new stream only, or some or all of the data for the current stream, but never both. So a basic small file containing
     * nothing but a single stream will be read in two calls.
     * 
     * Note that it is the responsibility of the caller to have already opened a valid descriptor to the file containing the data
     * BackupRead requires a final 'cleanup' call after the data has all been read, and this will be done if necessary.
     */
    public BackupReader(IntPtr fromFd, IntPtr fromBkFd) // fromFd is an open file to read from
    {
        this.fromFd = fromFd;
        this.fromBkFd = fromBkFd;
        if (this.fromBkFd == W32File.INVALID_HANDLE_VALUE) readingBkFile = false;
    }
    public bool readNextStreamPart()
    {
        atNewStreamHeader = false;
        if (atEndOfFile) return false;
        uint desiredReadSize = 1024 * 1024;
        uint bytesRead = 0;
        // if the current buffer has all been used up, get some more data if possible
        if (pBufferSize == pBufferPos)
        {
            pBufferSize = 0;
            pBufferPos = 0;
        }
        // our buffer is empty so get some more data
        if (pBufferSize == 0)
        {
            if (entireFileRead)
            {
                // no point trying again
                atEndOfFile = true; return false;
            }
            bool readSucceeded = false;
            if (!readingBkFile)
            {
                readSucceeded = W32File.BackupRead(fromFd,
                pBuffer,
                desiredReadSize,
                out bytesRead,
                false,
                true,
                ref rcontext);
            }
            else
            {
                readSucceeded = W32File.ReadFile(fromBkFd, pBuffer, desiredReadSize, out bytesRead, IntPtr.Zero);
            }
            if (!readSucceeded)
            {
                failed = true; return false;
            }
            if (bytesRead == 0)
            {
                if (readingBkFile)
                {
                    readingBkFile = false;
                    return readNextStreamPart();
                }

                entireFileRead = true;
                atEndOfFile = true; return false;
            }
            if (desiredReadSize > bytesRead) // so we've got more data now, but no more to come
            {
                if (readingBkFile)
                {
                    readingBkFile = false;
                }
                else
                {
                    entireFileRead = true;
                }
            }
            pBufferSize = bytesRead;
            //Console.Error.WriteLine("Read {0}", bytesRead);
        }
        // so we have some data in a buffer - either cos we've just read it or cos it was left over from last time
        // are we at the start of a brand new stream?
        if (atLastSegmentOfCurrentStream || atStartOfFile)
        {
            // at this point, we are either at the end of the file, or we are at the start of the stream
            // if we are unlucky, we have some but not all of the next stream header. So to prevent any problems, we check for this 
            atStartOfFile = false;
            atLastSegmentOfCurrentStream = false;
            //Console.Error.WriteLine("CHECKING:{0} in buffer", pBufferSize - pBufferPos);
            if (!entireFileRead && (pBufferSize - pBufferPos < 1024))
            {
                IntPtr pBufferEnd = new IntPtr(pBuffer.ToInt64() + pBufferSize);
                bytesRead = 0;
                bool readSucceeded = false;
                if (!readingBkFile)
                {
                    readSucceeded = W32File.BackupRead(fromFd,
                    pBufferEnd,
                    1024,
                    out bytesRead,
                    false,
                    true,
                    ref rcontext);
                }
                else
                {
                    readSucceeded = W32File.ReadFile(fromBkFd, pBufferEnd, 1024, out bytesRead, IntPtr.Zero);
                }
                pBufferSize += bytesRead;
            }
            if (pBufferSize == pBufferPos)
            {
                // we have no more data so we've reached the end of the file
                entireFileRead = true;
                atEndOfFile = true;
                return false; // there's no data to give
            }
            streamDataSegmentStart = new IntPtr(pBuffer.ToInt64() + (Int64)pBufferPos);
            streamHeader = (W32File.WIN32_STREAM_ID)Marshal.PtrToStructure(streamDataSegmentStart, typeof(W32File.WIN32_STREAM_ID));
            currentStreamNameSize = (int)streamHeader.StreamNameSize;
            currentStreamName = Marshal.PtrToStringUni(new IntPtr(streamDataSegmentStart.ToInt64() + W32File.MIN_WIN32_STREAM_ID_SIZE), currentStreamNameSize / 2);

            streamDataSegmentSize = (uint)(W32File.MIN_WIN32_STREAM_ID_SIZE + currentStreamNameSize);
            pBufferPos += streamDataSegmentSize;
            currentStreamDataPos = 0;
            atLastSegmentOfCurrentStream = streamHeader.Size == 0; // if the stream is empty there's only a header
            atNewStreamHeader = true;
            return true; // The first time we call the function on a new stream we just get the header
        }
        // if we get here, it means that we are somewhere in the reading of the actual stream data

        if (pBufferSize == pBufferPos) // oh dear, there's no more data to be had. Maybe we're just on a read boundary. 
        {
            return readNextStreamPart(); // so we recurse and have one last go
        }
        UInt64 dataAvailable = pBufferSize - pBufferPos;
        if (dataAvailable > streamHeader.Size - currentStreamDataPos) dataAvailable = streamHeader.Size - currentStreamDataPos;
        streamDataSegmentStart = new IntPtr(pBuffer.ToInt64() + (Int64)pBufferPos);
        streamDataSegmentSize = (uint)dataAvailable;
        pBufferPos += streamDataSegmentSize;
        currentStreamDataPos += dataAvailable;
        if (currentStreamDataPos == streamHeader.Size)
        {
            atLastSegmentOfCurrentStream = true;
        }
        return true;
    }

}
public class FileInfo
{
    public W32File.BY_HANDLE_FILE_INFORMATION w32fileinfo = new W32File.BY_HANDLE_FILE_INFORMATION();
    public String checksum = "";
    public Boolean excluded = false;
    public Boolean hasBkfd = false;
    public Boolean failedToOpen = true;
}
public enum ReportStage
{
    Starting = 1,
    OpenOriginal = 2,
    OpenNew = 3,
    CreateTarget = 4,
    Transfer = 6,
    CleanUp = 8,
    OpenBkf = 11,
    Deleting = 12,
    Finished = 100
}
public enum Outcome
{
    NotFinished = -1,
    IgnoredSource = 0,
    // Now we have the cases where the destination doesn't already exist
    NewFile = 1,
    NewFileSymlink = 2,
    NewDirectorySymlink = 3,
    NewDirectory = 5,
    NewHardlinkToSource = 6,
    NewHardlinkInDestination = 7,
    // Now the cases where both exist
    ReplaceFileOrSymlinkFileWithFile = 31,
    ReplaceFileOrSymlinkFileWithFileSymlink = 32,
    ReplaceFileOrSymlinkFileWithDirectorySymlink = 33,
    ReplaceFileOrSymlinkFileWithDirectory = 34,
    ReplaceFileOrSymlinkFileWithHardLinkToSource = 35,
    ReplaceFileOrSymlinkFileWithHardLinkInDestination = 36,

    ReplaceDirectorySymlinkWithFile = 41,
    ReplaceDirectorySymlinkWithFileSymlink = 42,
    ReplaceDirectorySymlinkWithDirectorySymlink = 43,
    ReplaceDirectorySymlinkWithDirectory = 44,
    ReplaceDirectorySymlinkWithHardLinkToSource = 45,
    ReplaceDirectorySymlinkWithHardLinkInDestination = 46,

    ReplaceDirectoryWithFile = 51,
    ReplaceDirectoryWithFileSymlink = 52,
    ReplaceDirectoryWithDirectorySymlink = 53,
    ReplaceDirectoryWithDirectory = 54,
    ReplaceDirectoryWithHardLinkToSource = 55,
    ReplaceDirectoryWithHardLinkInDestination = 56,

    PreserveExtra = 61,
    DeleteExtraFile = 62,
    DeleteExtraDirectory = 63,
    DeleteFileWithinReplacedDirectory = 67,
    DeleteDirectoryWithinReplacedDirectory = 68,

    Unchanged = 81,
    OnlyNeedBkFile = 82,
    // bad outcomes, failed means failed and did nothing, failedAndBroke means did something but not the right thing!
    Failed = 90,
    FailedAndBroke = 91
}
/* 
    case Outcome.NotFinished:
    case Outcome.Reported:
    case Outcome.IgnoredSource:
    case Outcome.NewFile:
    case Outcome.NewFileSymlink:
    case Outcome.NewDirectorySymlink:
    case Outcome.NewDirectory:
    case Outcome.NewHardlinkToSource:
    case Outcome.NewHardlinkInDestination:
    case Outcome.ReplaceFileOrSymlinkFileWithFile:
    case Outcome.ReplaceFileOrSymlinkFileWithFileSymlink:
    case Outcome.ReplaceFileOrSymlinkFileWithDirectorySymlink:
    case Outcome.ReplaceFileOrSymlinkFileWithDirectory:
    case Outcome.ReplaceFileOrSymlinkFileWithHardLinkToSource:
    case Outcome.ReplaceFileOrSymlinkFileWithHardLinkInDestination:

    case Outcome.ReplaceDirectorySymlinkWithFile:
    case Outcome.ReplaceDirectorySymlinkWithFileSymlink:
    case Outcome.ReplaceDirectorySymlinkWithDirectorySymlink:
    case Outcome.ReplaceDirectorySymlinkWithDirectory:
    case Outcome.ReplaceDirectorySymlinkWithHardLinkToSource:
    case Outcome.ReplaceDirectorySymlinkWithHardLinkInDestination:

    case Outcome.ReplaceDirectoryWithFile:
    case Outcome.ReplaceDirectoryWithFileSymlink:
    case Outcome.ReplaceDirectoryWithDirectorySymlink:
    case Outcome.ReplaceDirectoryWithDirectory:
    case Outcome.ReplaceDirectoryWithHardLinkToSource:
    case Outcome.ReplaceDirectoryWithHardLinkInDestination:

    case Outcome.PreserveExtra:,
    case Outcome.DeleteExtraFile:
    case Outcome.DeleteExtraDirectory:
    case Outcome.DeleteFileWithinReplacedDirectory:
    case Outcome.DeleteDirectoryWithinReplacedDirectory:
    case Outcome.Unchanged:
    case Outcome.OnlyNeedBkFile:
    case Outcome.Failed:
    case Outcome.FailedAndBroke:
 */
public class FileDisposition
{
    public String pathName = null;
    public FileInfo fromFileInfo = null;
    public FileInfo toFileInfo = null;
    public Outcome desiredOutcome = Outcome.NotFinished;
    public Outcome actualOutcome = Outcome.NotFinished;
    public Boolean resultReported = false;
    public Win32Exception exception = null;
}
public class Backup
{
    // Options
    // What to work with - almost always entire hierarchy but sometimes just one file/dir by itself
    public Boolean recursive = true;
    public Boolean useHardLinks = false; // if possible link source to dest rather than copying
    public Boolean cloneReparse = true; // if true we clone reparse points. This requires the destination to be NTFS
    public Boolean cloneFile = true; // if true we clone normal files
    public Boolean cloneDir = true; // if true we clone directories ( and their contents if recursive )
    public Boolean cloneAlsoUsesBkf = false; // if true, we look for extra information and use this in addition to the source file ( if any )

    // What to produce on the other side
    public Boolean restoreFileContents = true; // if true, we create an output file with the same contents as the input file
    public Boolean restoreAFS = true; // if true, the output file will also contain any AFS ( named streams ) in the input. Destination must be NTFS
    public Boolean restorePermissions = false; // if true, the output file will have the identical permissions to the input file. This can have unintended consequences.
    public Boolean restoreAttributes = true; // if true, the output file will recreate attributes such as readonly, system, hidden
    public Boolean restoreTimes = true; // if true, the output file will have the last mod time etc copied from the source
    public Boolean restoreHardLinks = true; // if true we notice when source files are hard links within the hierarchy being copied, 
    // and if so we restore them as hardlinks on the other side


    public Boolean createBkf = false; // if true, all data apart from file contents is written to an extra location to allow a restore to recreate the source even if the destination
    // isn't NTFS. This applies to permissions, attributes and reparse info, but not by default either file contents or AFS
    public Boolean createBkfAFS = false; // if true, the extra location will contain any AFS in addition to permissions and attributes
    public Boolean createBkfData = false; // if true, the file data is written to the extra location


    // How to treat what's already in the destination if anything
    public Boolean overwriteDir = false; // if the destination is a directory whereas the source is a link or a normal file, allow it to be changed, risking the loss of the current
    // contents of the directory. Risky so off by default.
    public Boolean overwriteReparse = true; // if the destination is a reparse whereas the source is not, allow it to be overwritten. This risks much less.
    public Boolean overwriteFile = false; // if the destination is an existing file, allow it to be changed into a dir or a reparse
    public Boolean overwriteSameType = true; // if source and destination are both files, both dirs or both reparses, change the destination to be the same as the source. 

    public Boolean removeExtra = false;  // delete any files in the destination but not in the source
    public List<String> excludeList = null; // list of files to exclude.

    public System.IO.StreamWriter logFile = null;

    public String paramOriginalPath;  // the param is what is passed to the backup, the others are the regularised unicoded versions
    public String paramNewPath;
    public String sourcePath;
    public String newPath;
    public String sourcePathDrive;
    public String sourcePathAfterDrive;
    public String newPathDrive;
    public String newPathAfterDrive;

    private const String bkFileSuffix = ".bkfd";
    private const String unicodePrefix = @"\\?\";

    public Hashtable filesToKeep = new Hashtable();
    public SortedDictionary<String, FileInfo> fromFileList = new SortedDictionary<string, FileInfo>(), toFileList = new SortedDictionary<string, FileInfo>();
    public SortedDictionary<String, FileDisposition> actionList = new SortedDictionary<string, FileDisposition>();

    public Outcome outcome;
    public ReportStage reportStage;
    public int nFiles = 0;
    public int nDirs = 0;
    public int nSpecial = 0;
    public int nFailed = 0;
    public int nSame = 0;
    public int nIgnored = 0;
    public int nExcluded = 0;
    public int nCopied = 0;
    public int nDeleted = 0;
    public int nInternalHardLinked = 0;

    // store hard link information as needed
    private Hashtable hardlinkInfo = new Hashtable();
    public const int CHECKSUM_SIZE = 32;
    public DeletionHelper deletionHelper = new DeletionHelper(); // public so we can override the reporting if we wish
    String directoryBeingMonitored = "";
    Boolean directoryIsBeingDeleted = false;
    Boolean directoryIsBeingPreserved = false;
    private String addUnicodePrefix(String filename)
    {
        if (filename.StartsWith(unicodePrefix))
        {
            return filename;
        }
        if (filename.StartsWith(@"\\")) // this is a normal UNC
        {
            return unicodePrefix + "UNC" + filename.Substring(1);
        }
        if (filename.StartsWith("UNC"))
        {
            return unicodePrefix + filename;
        }

        return unicodePrefix + filename;
    }
    protected virtual Boolean exclude(String filename)
    {
        // the default excluder - can be overriden in derived classes
        // return true if this file should not be copied based on its name only
        if (excludeList == null) return false;
        //        if (filename.Length <= sourcePathLength) return false;
        String pathToMatch = @"\" + filename + @"\";
        pathToMatch = pathToMatch.ToUpper();
        int l = pathToMatch.Length;
        for (int i = 0; i < l - 1; i++)
        {
            String tryHere = pathToMatch.Substring(i);
            int index = excludeList.BinarySearch(tryHere);
            if (index >= 0)
            {
                // exact match
                return true;
            }

            index = ~index;
            if (index == 0) continue; // we are smaller than the first element
            index = index - 1;
            String nearest = excludeList[index];
            if (pathToMatch.Contains(nearest)) return true;
        }
        return false;
    }
    public String fixUpPath(String fromPath)
    {
        // Now, we have to try and regularize the path names to avoid unexpected surprises.
        // Windows is a complete mess here
        // 
        // trailing \ is mandatory if we are on the root of a drive, whether specified by drive letter or unicode path
        // trailing \ must not be present if we are on any other directory or filename
        // telling the above two cases apart is non-trivial, but we want to be nice to our users and let them bung unnecessary \ in if they feel like it
        // as this makes it easier to script with
        // multiple \ are always bad unless they are the first two characters of the filename, in which case they indicate a unc or unicode path
        // We get round all this nonsense by passing to backup a path which never contains \\ except possibly at the beginning, 
        // and never ends in \ even though it might have to before we open it ( e.g. c: )
        StringBuilder newFromPath = new StringBuilder(fromPath.Length);
        Boolean lastWasBackslash = false;
        Boolean atFirstChar = true;
        Boolean deferringBackslash = false;
        foreach (Char c in fromPath)
        {
            if (atFirstChar)
            {
                if (c == '\\')
                    newFromPath.Append(c);

                atFirstChar = false;
            }
            if (c == '\\')
            {
                if (lastWasBackslash) continue; // this is the second or more in a sequence of backslashes
                lastWasBackslash = true;
                deferringBackslash = true;
                continue;
            }
            lastWasBackslash = false;
            if (deferringBackslash)
            {
                deferringBackslash = false;
                newFromPath.Append('\\');
            }
            newFromPath.Append(c);
        }
        return newFromPath.ToString();
    }

    public String drivePart(String s) // find the initial part of a string which represents the drive part up to but not including a \ for the root
    {
        Match m;
        int i;
        m = Regex.Match(s, @"^\\\\\?\\\w:", RegexOptions.IgnoreCase);
        i = m.Length;
        if (i > 0)
        {
            return s.Substring(0, i);
        }
        m = Regex.Match(s, @"^\\\\\?\\Volume{\w*}", RegexOptions.IgnoreCase);
        i = m.Length;
        if (i > 0)
        {
            return s.Substring(0, i);
        }
        m = Regex.Match(s, @"^\\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy[0-9]*", RegexOptions.IgnoreCase);
        i = m.Length;
        if (i > 0)
        {
            return s.Substring(0, i);
        }

        return "";
    }
    public String prettyPrint(FileInfo fi)
    {
        if (fi == null)
        {
            return "none";
        }
        var fa = fi.w32fileinfo.FileAttributes;

        if (fa.HasFlag(FileAttributes.ReparsePoint))
        {
            return fa.HasFlag(FileAttributes.Directory) ? "sdir" : "sfil";
        }
        return fa.HasFlag(FileAttributes.Directory) ? "dir " : "file";
    }
    public String prettyPrintOutcome(Outcome finalOutcome)
    {
        switch (finalOutcome)
        {
            case Outcome.NotFinished: return ("....");
            case Outcome.IgnoredSource: return ("Igno");
            case Outcome.NewFile: return ("Copy");
            case Outcome.NewFileSymlink: return ("Copy");
            case Outcome.NewDirectorySymlink: return ("Copy");
            case Outcome.NewDirectory: return ("NDir");
            case Outcome.NewHardlinkToSource: return ("HLnk");
            case Outcome.NewHardlinkInDestination: return ("HLnk");
            case Outcome.ReplaceFileOrSymlinkFileWithFile: return ("Copy");
            case Outcome.ReplaceFileOrSymlinkFileWithFileSymlink: return ("Copy");
            case Outcome.ReplaceFileOrSymlinkFileWithDirectorySymlink: return ("Copy");
            case Outcome.ReplaceFileOrSymlinkFileWithDirectory: return ("Copy");
            case Outcome.ReplaceFileOrSymlinkFileWithHardLinkToSource: return ("HLnk");
            case Outcome.ReplaceFileOrSymlinkFileWithHardLinkInDestination: return ("HLnk");

            case Outcome.ReplaceDirectorySymlinkWithFile: return ("Copy");
            case Outcome.ReplaceDirectorySymlinkWithFileSymlink: return ("Copy");
            case Outcome.ReplaceDirectorySymlinkWithDirectorySymlink: return ("Copy");
            case Outcome.ReplaceDirectorySymlinkWithDirectory: return ("Copy");
            case Outcome.ReplaceDirectorySymlinkWithHardLinkToSource: return ("HLnk");
            case Outcome.ReplaceDirectorySymlinkWithHardLinkInDestination: return ("HLnk");

            case Outcome.ReplaceDirectoryWithFile: return ("Copy");
            case Outcome.ReplaceDirectoryWithFileSymlink: return ("Copy");
            case Outcome.ReplaceDirectoryWithDirectorySymlink: return ("Copy");
            case Outcome.ReplaceDirectoryWithDirectory: return ("Copy");
            case Outcome.ReplaceDirectoryWithHardLinkToSource: return ("HLnk");
            case Outcome.ReplaceDirectoryWithHardLinkInDestination: return ("HLnk");

            case Outcome.PreserveExtra: return ("Prsv");
            case Outcome.DeleteExtraFile: return ("DExf");
            case Outcome.DeleteExtraDirectory: return ("DExd");
            case Outcome.DeleteFileWithinReplacedDirectory: return ("DExt");
            case Outcome.DeleteDirectoryWithinReplacedDirectory: return ("DExt");
            case Outcome.Unchanged: return ("Same");
            case Outcome.OnlyNeedBkFile: return ("SamK");
            case Outcome.Failed: return ("Fail");
            case Outcome.FailedAndBroke: return ("Brkn");
        }

        return ("EEEE");
    }
    public Backup(String of, String nf)
    {
        paramOriginalPath = of;
        paramNewPath = nf;
        char[] backslashes = { '\\' };
        // Because we support long filenames, we use windows functions which don't understand forward slashes. But because we do support forward slashes, we convert them here
        sourcePath = paramOriginalPath.Replace(@"/", @"\");
        newPath = paramNewPath.Replace(@"/", @"\");
        sourcePath = addUnicodePrefix(fixUpPath(sourcePath));
        newPath = addUnicodePrefix(fixUpPath(newPath));

        sourcePathAfterDrive = "";
        sourcePathDrive = drivePart(sourcePath);
        if (sourcePathDrive.Length < sourcePath.Length) sourcePathAfterDrive = sourcePath.Substring(sourcePathDrive.Length);

        newPathAfterDrive = "";
        newPathDrive = drivePart(newPath);
        if (newPathDrive.Length < newPath.Length) newPathAfterDrive = newPath.Substring(newPathDrive.Length);
        // at this point, the path is regularised as a unicode prefix with no repeated slashes and no trailing slash.
        // We deal with the cases where we might need a trailing slash to open the file later
    }

    public static int reportVerbosity = 3; // quiet = 1 normal = 3 verbose = 5 veryverbose = 8

    // override this in a derived class if you want to report differently
    protected virtual void reporter(FileDisposition disposition)
    {
        String filename = disposition.pathName;
        if (disposition.resultReported) return;

        if (disposition.actualOutcome == Outcome.NotFinished) return;
        if (disposition.exception == null)
        {
            if (logFile != null) logFile.WriteLine("{0,-4}->{1,-4}:{2,-6}:{3}", prettyPrint(disposition.fromFileInfo),
                           prettyPrint(disposition.toFileInfo), prettyPrintOutcome(disposition.actualOutcome), filename);
            if (reportVerbosity > 3)
            {
                Console.Error.WriteLine("{0,-4}->{1,-4}:{2,-6}:{3}", prettyPrint(disposition.fromFileInfo),
                            prettyPrint(disposition.toFileInfo), prettyPrintOutcome(disposition.actualOutcome), filename);

            }
        }
        else
        {
            if (logFile != null) logFile.WriteLine("{0,-4}->{1,-4}:{3,-6}:{5}:{2:-6}:{4}", prettyPrint(disposition.fromFileInfo),
                          prettyPrint(disposition.toFileInfo), prettyPrintOutcome(disposition.desiredOutcome), prettyPrintOutcome(disposition.actualOutcome), filename,
                          disposition.exception.Message);
            if (reportVerbosity > 1)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Error.WriteLine("{0,-4}->{1,-4}:{3,-6}:{5}:{2:-6}:{4}", prettyPrint(disposition.fromFileInfo),
                            prettyPrint(disposition.toFileInfo), prettyPrintOutcome(disposition.desiredOutcome), prettyPrintOutcome(disposition.actualOutcome), filename,
                            disposition.exception.Message);
                Console.ResetColor();
            }
        }
        disposition.resultReported = true;
    }

    // public delegate void reportFunc(String sourceFilename, String newFilename, Boolean isDir, Boolean isSpecial, long filelen, long donesofar, reportStage stage, outcome finalOutcome, Win32Exception e);
    // public reportFunc reporter;

    public delegate Boolean cloneFunc(String sourceFilename, FileFind.WIN32_FIND_DATA fdata, SortedDictionary<String, FileInfo> fileList);

    virtual public void doit()
    {
        Boolean sourceIsDir = false;
        Boolean sourceIsReparse = false;
        Boolean sourceIsDrive = false;


        if (excludeList != null)
        {
            excludeList.Sort();
        }

        // Our fist task is to work out what to do about our top-level objects, because they might be drives not directories
        // and windows is so very inconsistent about such things
        IntPtr sourcePathFd = W32File.INVALID_HANDLE_VALUE;
        IntPtr destPathFd = W32File.INVALID_HANDLE_VALUE;

        FileFind.WIN32_FIND_DATA sourceFdata = new FileFind.WIN32_FIND_DATA();
        FileFind.WIN32_FIND_DATA destFdata = new FileFind.WIN32_FIND_DATA(); 

        if (this.sourcePath == this.sourcePathDrive)
        {
            sourceIsDrive = true; sourceIsDir = true;
            sourceFdata.cFileName = "";
            sourceFdata.dwFileAttributes = (int)(FileAttributes.Directory | FileAttributes.Device);
            sourcePathFd = W32File.CreateFile(sourcePath + @"\"
             , W32File.EFileAccess.GenericRead
             , W32File.EFileShare.Read
             , IntPtr.Zero
             , W32File.ECreationDisposition.OpenExisting
             , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
             , IntPtr.Zero);
            if (sourcePathFd == W32File.INVALID_HANDLE_VALUE)
            {
                Console.ForegroundColor = (ConsoleColor.Magenta);
                Console.Error.WriteLine("Source drive {0} appears nonexistent or empty - aborting", sourcePath);
                Console.Error.WriteLine("{0}", new Win32Exception().Message);
                Console.ResetColor();
                nFailed = 1;
                return;
            }
            W32File.CloseHandle(sourcePathFd);

        }
        else
        {
            W32File.WIN32_FILE_ATTRIBUTE_DATA sourceAttributeData = new W32File.WIN32_FILE_ATTRIBUTE_DATA() ;
            Boolean gotFileAttributesEx = W32File.GetFileAttributesEx(sourcePath, W32File.GET_FILEEX_INFO_LEVELS.GetFileExInfoStandard, out sourceAttributeData);
            FileAttributes i = (FileAttributes) sourceAttributeData.dwFileAttributes;

            sourceFdata.cFileName = sourcePathAfterDrive;
            sourceFdata.dwFileAttributes = (int) i;
            sourceFdata.nFileSizeHigh = sourceAttributeData.nFileSizeHigh;
            sourceFdata.nFileSizeLow = sourceAttributeData.nFileSizeLow;
            sourceFdata.ftLastWriteTime = sourceAttributeData.ftLastWriteTime;

            if ((int)i == -1 || !gotFileAttributesEx)
            {
                Console.ForegroundColor = (ConsoleColor.Magenta);
                Console.Error.WriteLine("Source {0} appears nonexistent or empty - aborting", sourcePath);
                Console.Error.WriteLine("{0}", new Win32Exception().Message);
                Console.ResetColor();
                nFailed = 1;
                return;
            }

            // get accurate information about the source file 
            if (i.HasFlag(FileAttributes.Directory))
            {
                sourceIsDir = true;
            }
            if (i.HasFlag(FileAttributes.ReparsePoint))
            {
                sourceIsReparse = true;
            }
        }


        if (this.newPath == this.newPathDrive)
        {
            destFdata.cFileName = "";
            destFdata.dwFileAttributes = (int)(FileAttributes.Device | FileAttributes.Directory);

            if (!sourceIsDir || sourceIsReparse)
            {
                Console.ForegroundColor = (ConsoleColor.Magenta);
                Console.Error.WriteLine("Your source is not a directory but your destination is a drive - aborting", sourcePath);
                Console.Error.WriteLine("Please specify the destination as a full filename if you just want to copy a single item");
                Console.ResetColor();
                nFailed = 1;
                return;
            }
            destPathFd = W32File.CreateFile(newPath + @"\"
             , W32File.EFileAccess.GenericRead
             , W32File.EFileShare.Read
             , IntPtr.Zero
             , W32File.ECreationDisposition.OpenExisting
             , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
             , IntPtr.Zero);
            if (destPathFd == W32File.INVALID_HANDLE_VALUE)
            {
                Console.ForegroundColor = (ConsoleColor.Magenta);
                Console.Error.WriteLine("Destination drive {0} appears nonexistent or inaccessible - aborting", newPath);
                Console.Error.WriteLine("{0}", new Win32Exception().Message);
                Console.ResetColor();
                nFailed = 1;
                return;
            }
            else
            {
                W32File.CloseHandle(destPathFd);
            }
        }
        else
        {
            // the destination is not a drive 
            var i = W32File.GetFileAttributes(newPath);

            if ((int)i == -1)
            {
                // the destination doesn't exist yet. So try to create it
                if (sourceIsDir)
                {
                    if (!W32File.CreateDirectory(newPath, IntPtr.Zero))
                    {
                        Console.ForegroundColor = (ConsoleColor.Magenta);
                        Console.Error.WriteLine("Unable to create destination directory {0} - aborting", newPath);
                        Console.Error.WriteLine("{0}", new Win32Exception().Message);
                        Console.ResetColor();
                        nFailed = 1;
                        return;
                    }

                }
                else
                {
                    destPathFd = W32File.CreateFile(newPath
             , W32File.EFileAccess.GenericRead
             , W32File.EFileShare.Read
             , IntPtr.Zero
             , W32File.ECreationDisposition.New
             , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
             , IntPtr.Zero);
                    if (destPathFd == W32File.INVALID_HANDLE_VALUE)
                    {
                        Console.ForegroundColor = (ConsoleColor.Magenta);
                        Console.Error.WriteLine("Unable to create destination file {0} - aborting", newPath);
                        Console.Error.WriteLine("{0}", new Win32Exception().Message);
                        Console.ResetColor();
                        nFailed = 1;
                        return;
                    }
                    else
                    {
                        W32File.CloseHandle(destPathFd);
                    }
                }
                i = W32File.GetFileAttributes(newPath);
                destFdata.cFileName = newPathAfterDrive;
                destFdata.dwFileAttributes = (int)i;

            }
        }

        CloneRecursive(sourcePath, "", sourceFdata, fromFileList, CloneGetSourceFiles);

  

        CloneRecursive(newPath, "", destFdata, toFileList, CloneGetDestFiles);

        if (reportVerbosity > 4) Console.Error.WriteLine("SourceFileList contains {0} entries", fromFileList.Count);
        if (reportVerbosity > 4) Console.Error.WriteLine("TargetFileList contains {0} entries", toFileList.Count);

        // So we have two nice lists of files
        // The next stage is to work out what needs to be done

        foreach (String fn in fromFileList.Keys)
        {
            // we treat .bkfd files specially
            if (fn.EndsWith(bkFileSuffix))
            {
                String mainFn = fn.Substring(0, fn.Length - bkFileSuffix.Length);
                if (actionList.ContainsKey(mainFn))
                {
                    actionList[mainFn].fromFileInfo.hasBkfd = true;
                }
            }
            else
            {
                FileDisposition fdisp = new FileDisposition();
                fdisp.fromFileInfo = fromFileList[fn];
                fdisp.pathName = fn;
                actionList.Add(fn, fdisp);
            }
        }

        foreach (String fn in toFileList.Keys)
        {
            if (fn.EndsWith(bkFileSuffix))
            {
                String mainFn = fn.Substring(0, fn.Length - bkFileSuffix.Length);
                if (actionList.ContainsKey(mainFn))
                {
                    var v = actionList[mainFn].toFileInfo;
                    if (v != null)
                    {
                        v.hasBkfd = true;
                    }
                }
            }
            else
            {
                FileDisposition fdisp;
                if (actionList.ContainsKey(fn))
                {
                    fdisp = actionList[fn];
                }
                else
                {
                    fdisp = new FileDisposition();
                    fdisp.pathName = fn;
                    actionList.Add(fn, fdisp);
                }
                fdisp.toFileInfo = toFileList[fn];
            }
        }
        List<FileDisposition> toDelete = new List<FileDisposition>();
        List<FileDisposition> toMkdir = new List<FileDisposition>();
        List<FileDisposition> toDeleteAtEnd = new List<FileDisposition>();
        if (reportVerbosity > 7) Console.Error.WriteLine("Calcuating decision tree");
        foreach (KeyValuePair<String, FileDisposition> action in actionList)
        {
            CloneDecide(action.Value);
            switch (action.Value.desiredOutcome)
            {
                case Outcome.DeleteDirectoryWithinReplacedDirectory:
                case Outcome.DeleteFileWithinReplacedDirectory:
                    toDelete.Add(action.Value);
                    break;

                case Outcome.ReplaceDirectorySymlinkWithDirectory:
                case Outcome.ReplaceDirectorySymlinkWithDirectorySymlink:
                case Outcome.ReplaceDirectoryWithDirectorySymlink:
                case Outcome.ReplaceDirectorySymlinkWithFile:
                case Outcome.ReplaceDirectorySymlinkWithFileSymlink:
                case Outcome.ReplaceDirectorySymlinkWithHardLinkInDestination:
                case Outcome.ReplaceDirectorySymlinkWithHardLinkToSource:

                case Outcome.ReplaceDirectoryWithFile:
                case Outcome.ReplaceDirectoryWithFileSymlink:
                case Outcome.ReplaceDirectoryWithHardLinkInDestination:
                case Outcome.ReplaceDirectoryWithHardLinkToSource:
                case Outcome.ReplaceFileOrSymlinkFileWithDirectory:
                case Outcome.ReplaceFileOrSymlinkFileWithDirectorySymlink:
                case Outcome.ReplaceFileOrSymlinkFileWithFileSymlink:
                case Outcome.ReplaceFileOrSymlinkFileWithHardLinkInDestination:
                case Outcome.ReplaceFileOrSymlinkFileWithHardLinkToSource:
                    toDelete.Add(action.Value);
                    break;

                case Outcome.DeleteExtraDirectory:
                case Outcome.DeleteExtraFile:
                    toDeleteAtEnd.Add(action.Value);
                    break;

                case Outcome.ReplaceFileOrSymlinkFileWithFile: // special case - defer deletion of target so we can optimize if necessary
                    break;
                case Outcome.IgnoredSource:
                case Outcome.NewFile:
                case Outcome.NewFileSymlink:
                case Outcome.NewHardlinkInDestination:
                case Outcome.NewHardlinkToSource:
                case Outcome.OnlyNeedBkFile:
                case Outcome.PreserveExtra:
                case Outcome.Unchanged:
                    break;
            }
        }
        int nDeletions = toDelete.Count;
        for (int i = nDeletions - 1; i >= 0; i--)
        {
            var v = toDelete[i];
            CloneDelete(v);
            nDeleted++;
            reporter(v);
        }
        if (reportVerbosity > 7) Console.Error.WriteLine("Initial deletions completed. Starting main actions.");
        // Do the rest of the work
        foreach (KeyValuePair<String, FileDisposition> action in actionList)
        {
            // Console.Error.WriteLine("{0} start", action.Value.pathName);
            CloneAct(action.Value);
            switch (action.Value.actualOutcome)
            {
                case Outcome.Failed:
                case Outcome.FailedAndBroke:

                    nFailed++;
                    break;
                case Outcome.IgnoredSource:
                    break; // we've already counted these

                case Outcome.NewHardlinkInDestination:
                case Outcome.ReplaceFileOrSymlinkFileWithHardLinkInDestination:
                case Outcome.ReplaceDirectorySymlinkWithHardLinkInDestination:
                case Outcome.ReplaceDirectoryWithHardLinkInDestination:
                    nInternalHardLinked++;
                    break;

                case Outcome.NewFile:
                case Outcome.ReplaceFileOrSymlinkFileWithFile:
                case Outcome.ReplaceDirectorySymlinkWithFile:
                case Outcome.ReplaceDirectoryWithFile:
                    nCopied++;
                    break;

                case Outcome.NewFileSymlink:
                case Outcome.ReplaceFileOrSymlinkFileWithFileSymlink:
                case Outcome.ReplaceDirectorySymlinkWithFileSymlink:
                case Outcome.ReplaceDirectoryWithFileSymlink:
                    nCopied++;
                    break;
                case Outcome.NewDirectorySymlink:
                case Outcome.ReplaceFileOrSymlinkFileWithDirectorySymlink:
                case Outcome.ReplaceDirectorySymlinkWithDirectorySymlink:
                case Outcome.ReplaceDirectoryWithDirectorySymlink:
                    nCopied++;
                    break;
                case Outcome.NewDirectory:
                case Outcome.ReplaceFileOrSymlinkFileWithDirectory:
                case Outcome.ReplaceDirectorySymlinkWithDirectory:
                case Outcome.ReplaceDirectoryWithDirectory:
                    nCopied++;
                    break;
                case Outcome.NewHardlinkToSource:
                case Outcome.ReplaceFileOrSymlinkFileWithHardLinkToSource:
                case Outcome.ReplaceDirectorySymlinkWithHardLinkToSource:
                case Outcome.ReplaceDirectoryWithHardLinkToSource:
                    nCopied++;
                    break;

                case Outcome.Unchanged:
                case Outcome.OnlyNeedBkFile:
                    nSame++;
                    break;


                case Outcome.PreserveExtra:
                case Outcome.DeleteExtraFile:
                case Outcome.DeleteExtraDirectory:
                case Outcome.DeleteFileWithinReplacedDirectory:
                case Outcome.DeleteDirectoryWithinReplacedDirectory:

                    break;
                case Outcome.NotFinished:
                    break;
            }
            reporter(action.Value); // report if finished at this point
            // Console.Error.WriteLine("{0} Finish", action.Value.pathName);
        }
        if (nFailed > 0)
        {
            if (reportVerbosity > 1) Console.Error.WriteLine("Errors - so not deleting extra files");
            // if we failed we don't delete extra files
        }
        else
        {
            if (reportVerbosity > 7) Console.Error.WriteLine("Deleting extra files if any");
            int nLateDeletions = toDeleteAtEnd.Count;
            for (int i = nLateDeletions - 1; i >= 0; i--)
            {
                var v = toDeleteAtEnd[i];
                CloneDelete(v);
                nDeleted++;
                reporter(v);
            }
        }

        return;
    }
    static ulong dsistart = 0;
    public void displayStreamInfo(String sourceFilename, IntPtr pBuffer, long sofar, long bytesRead)
    {

        if (sofar == 0)
        {
            dsistart = 0;
            // this is definitely a stream header
            W32File.WIN32_STREAM_ID wid1;
            wid1 = (W32File.WIN32_STREAM_ID)Marshal.PtrToStructure(pBuffer, typeof(W32File.WIN32_STREAM_ID));
            dsistart = wid1.Size;
            //Console.Error.WriteLine("DSI:{0}:{1}:{2}", wid1.StreamId, wid1.StreamNameSize, wid1.Size);
        }
    }
    public void CloneRecursive(string basePath, string fn1, FileFind.WIN32_FIND_DATA fdata, SortedDictionary<String, FileInfo> fileList, cloneFunc cloner)
    {
        Boolean shouldRecurse = false;
        if (fn1 == "")
        {
            // This is the annoying special case.
            // This means the call didn't come from cloneRecursive so fdata is probably nonsense. 
            // Also if fn1 is empty, then basePath might be a drive reference, and not an actual directory. In this case we MAY need to add a trailing 
            // backslash before passing the name to cloner, and we can't trust anything currently in fdata
            // The best solution appears to be to always add a '\', and if the open fails, try again without one
            String possibleSlash = "";
            FileAttributes fa = FileAttributes.Directory;
            if (drivePart(basePath) == basePath)
            {
                // If this is a drive, we try to open it to see if we can
                // This is the only time we open the file, but since it's a top-level special case, it's not a significant overhead
                possibleSlash = @"\";

                IntPtr pathExists = W32File.CreateFile(basePath + possibleSlash
                         , W32File.EFileAccess.GenericRead
                 , W32File.EFileShare.Read
                , IntPtr.Zero
                , W32File.ECreationDisposition.OpenExisting
               , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
                , IntPtr.Zero);
                if (pathExists == W32File.INVALID_HANDLE_VALUE) return;
                W32File.CloseHandle(pathExists);
            }
            else
            {
                fa = W32File.GetFileAttributes(basePath);
                if ((int)(fa) == -1) return;
            }

            fdata.dwFileAttributes = (int)fa;
            // Console.Error.WriteLine("CloneRecursive base is {0}", basePath + possibleSlash);
            shouldRecurse = cloner(possibleSlash, fdata, fileList);
        }
        else
        {
            shouldRecurse = cloner(fn1, fdata, fileList);
        }
        if (!shouldRecurse) return;
        FileFind.WIN32_FIND_DATA fdata1 = new FileFind.WIN32_FIND_DATA();
        IntPtr findhandle;
        findhandle = FileFind.FindFirstFile(basePath + fn1 + @"\*", ref fdata1);
        if (findhandle != W32File.INVALID_HANDLE_VALUE)
        {
            do
            {
                if ((fdata1.cFileName != @".") && (fdata1.cFileName != @".."))
                {
                    CloneRecursive(basePath, fn1 + @"\" + fdata1.cFileName, fdata1, fileList, cloner);
                }
            }
            while (FileFind.FindNextFile(findhandle, ref fdata1));
            FileFind.FindClose(findhandle);
        };
    }
    public bool CloneGetSourceFiles(String fileName, FileFind.WIN32_FIND_DATA fdata, SortedDictionary<String, FileInfo> fileList)
    {
        int origRow = Console.CursorTop;
        try
        {
            FileInfo fid = new FileInfo();
            if (fileName == @"\") fileName = "";
            if (exclude(fileName))
            {
                fid.excluded = true;
                if (reportVerbosity > 4) Console.Error.WriteLine("Excluding src {0}", fileName);
            }
            if (fileName.EndsWith(bkFileSuffix))
            {
                return false;
            }
            fileList.Add(fileName, fid);
            fid.w32fileinfo.CreationTime = fdata.ftCreationTime;
            fid.w32fileinfo.FileAttributes = (FileAttributes)fdata.dwFileAttributes;
            fid.w32fileinfo.FileIndexHigh = 0;
            fid.w32fileinfo.FileIndexLow = 0;
            fid.w32fileinfo.FileSizeHigh = fdata.nFileSizeHigh;
            fid.w32fileinfo.FileSizeLow = fdata.nFileSizeLow;
            fid.w32fileinfo.LastAccessTime = fdata.ftLastAccessTime;
            fid.w32fileinfo.LastAccessTime = fdata.ftLastWriteTime;
            fid.w32fileinfo.NumberOfLinks = 0;
            fid.w32fileinfo.VolumeSerialNumber = 0;

            if (fileList.Count % 10000 == 0)
            {
                if (reportVerbosity > 4) Console.Error.WriteLine("SourceFileList {0,-7} {1}", fileList.Count, fileName);
                //Console.SetCursorPosition(0, origRow);
            }

            return !fid.excluded && fid.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory) && !
                fid.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);
        }
        finally
        {

        }
    }
    public bool CloneGetDestFiles(String fileName, FileFind.WIN32_FIND_DATA fdata, SortedDictionary<String, FileInfo> fileList)
    {
        int origRow = Console.CursorTop;
        try
        {
            if (fileName == @"\") fileName = "";
            FileInfo fid = new FileInfo();
            if (exclude(fileName))
            {
                fid.excluded = true;
                if (reportVerbosity > 4) Console.Error.WriteLine("Excluding dest {0}", fileName);
            }
            fileList.Add(fileName, fid);
            fid.w32fileinfo.CreationTime = fdata.ftCreationTime;
            fid.w32fileinfo.FileAttributes = (FileAttributes)fdata.dwFileAttributes;
            fid.w32fileinfo.FileIndexHigh = 0;
            fid.w32fileinfo.FileIndexLow = 0;
            fid.w32fileinfo.FileSizeHigh = fdata.nFileSizeHigh;
            fid.w32fileinfo.FileSizeLow = fdata.nFileSizeLow;
            fid.w32fileinfo.LastAccessTime = fdata.ftLastAccessTime;
            fid.w32fileinfo.LastAccessTime = fdata.ftLastWriteTime;
            fid.w32fileinfo.NumberOfLinks = 0;
            fid.w32fileinfo.VolumeSerialNumber = 0;

            if (fileList.Count % 10000 == 0)
            {
                if (reportVerbosity > 4) Console.Error.WriteLine("DestFileList {0,-7} {1}", fileList.Count, fileName);
                //Console.SetCursorPosition(0, origRow);
            }

            return !fid.excluded && fid.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory) && !
                fid.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);

        }
        finally
        {

        }
    }
    public void CloneDecide(FileDisposition fdisp)
    {
        // We decide what we are going to do as far as possible with the information available
        // Since we haven't yet opened the file, there are some things we don't know, like whether it's locked, whether it's a hard link etc.
        // So the conclusion here is provisional

        String filename = fdisp.pathName;
        Boolean sourceExists = false;
        Boolean isDir = false;
        Boolean isReparse = false;
        Boolean isNormalDir = false;
        Boolean isReparseDir = false;
        Boolean isReparseFile = false;
        Boolean isNormalFile = false;
        Boolean fromExcluded = false;
        long flen = 0;

        Boolean destExists = false;
        Boolean dDir = false;
        Boolean dReparse = false;
        Boolean destIsNormalFile = false;
        Boolean destIsNormalDir = false;
        Boolean destIsReparseDir = false;
        Boolean destIsReparseFile = false;
        Boolean destExcluded = false;
        long dFlen = 0;


        if (fdisp.fromFileInfo != null)
        {
            sourceExists = true;
            fromExcluded = fdisp.fromFileInfo.excluded;
            isDir = fdisp.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory);
            isReparse = fdisp.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);
            isNormalFile = !isDir && !isReparse;
            isNormalDir = isDir && !isReparse;
            isReparseDir = isDir && isReparse;
            isReparseFile = !isDir && isReparse;
            flen = (((long)fdisp.fromFileInfo.w32fileinfo.FileSizeHigh) << 32) + fdisp.fromFileInfo.w32fileinfo.FileSizeLow;
        }
        if (fdisp.toFileInfo != null)
        {
            destExists = true;
            destExcluded = fdisp.toFileInfo.excluded;
            dDir = fdisp.toFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory);
            dReparse = fdisp.toFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);
            destIsNormalFile = !dDir && !dReparse;
            destIsNormalDir = dDir && !dReparse;
            destIsReparseDir = dDir && dReparse;
            destIsReparseFile = !dDir && dReparse;
            dFlen = (((long)fdisp.toFileInfo.w32fileinfo.FileSizeHigh) << 32) + fdisp.toFileInfo.w32fileinfo.FileSizeLow;
        }
        // edge case - we exclude a deeply nested file within a directory which is being replaced by a normal file
        if (fromExcluded || destExcluded) // exclusions trump all, and if this is a directory, the contents won't even be here
        {
            nIgnored++;
            fdisp.desiredOutcome = Outcome.IgnoredSource; return;
        }
        // this only works because our list is a SortedDictionary so directory contents always appear immediately after their parent
        if ((directoryIsBeingDeleted || directoryIsBeingPreserved) && !filename.StartsWith(directoryBeingMonitored))
        {
            directoryIsBeingPreserved = false;
            directoryIsBeingDeleted = false;
        }
        if (!sourceExists)
        {
            // This is an extra destination file
            // But it may also be a file within a directory hierarchy which needs to be deleted for other reasons
            if (directoryIsBeingDeleted)
            {
                fdisp.desiredOutcome = dDir ? Outcome.DeleteDirectoryWithinReplacedDirectory : Outcome.DeleteFileWithinReplacedDirectory;
            }
            else if (directoryIsBeingPreserved)
            {
                fdisp.desiredOutcome = Outcome.PreserveExtra;
            }
            else if (removeExtra && !fdisp.toFileInfo.excluded)
            {
                fdisp.desiredOutcome = dDir ? Outcome.DeleteExtraDirectory : Outcome.DeleteExtraFile;
            }
            else
            {
                fdisp.desiredOutcome = Outcome.PreserveExtra;
            }
            return; // if the source doesn't exist there's really nothing more to do here
        }



        if (isNormalFile) nFiles++;
        if (isNormalDir) nDirs++;
        if (isReparse) nSpecial++;

        // Now we make a determination
        if (!destExists)
        {
            fdisp.desiredOutcome = isNormalDir ? Outcome.NewDirectory : isReparseDir ? Outcome.NewDirectorySymlink : isReparseFile ? Outcome.NewFileSymlink
                : useHardLinks ? Outcome.NewHardlinkToSource : Outcome.NewFile;
        }
        // So both source and destination exist - 
        else
        {
            // Now see if we're interested in handling this kind of source file
            if (
                (isReparseDir && !cloneReparse) ||
                (isReparseFile && !cloneReparse) ||
                (isNormalDir && !cloneDir) ||
                (isNormalFile && !cloneFile)
                )
            {
                fdisp.desiredOutcome = Outcome.IgnoredSource;
            }
            if (
                (isReparse == dReparse) && (isDir == dDir)
                )
            {
                // so source and destination are the same type
                if (!overwriteSameType)
                {
                    fdisp.desiredOutcome = Outcome.PreserveExtra;
                }
                else if (isNormalDir ||
                    (isNormalFile && (flen == dFlen) && fdisp.fromFileInfo.w32fileinfo.LastWriteTime.Equals(fdisp.toFileInfo.w32fileinfo.LastWriteTime))
                    )
                {
                    // identical, or treated as such
                    fdisp.desiredOutcome = Outcome.Unchanged;
                    // we may still have to create a bkfd though
                    if (createBkf && !fdisp.toFileInfo.hasBkfd)
                    {
                        fdisp.desiredOutcome = Outcome.OnlyNeedBkFile;
                    }
                }
                else
                {
                    // so the files are the same type but not identical
                    fdisp.desiredOutcome = isNormalDir ? Outcome.ReplaceDirectoryWithDirectory
                            : isReparseFile ? Outcome.ReplaceFileOrSymlinkFileWithFileSymlink
                            : isReparseDir ? Outcome.ReplaceDirectorySymlinkWithDirectorySymlink
                            : useHardLinks ? Outcome.ReplaceFileOrSymlinkFileWithHardLinkToSource : Outcome.ReplaceFileOrSymlinkFileWithFile;
                }
            }
            // so the destination is not the same type as the source
            else if (destIsNormalDir && !overwriteDir)
            {
                fdisp.desiredOutcome = Outcome.PreserveExtra;
                // This is an interesting case because since we are not overwriting the directory, we'd better preserve its contents too, since they obviously don't exist
                // in the source as the source here isn't a directory
                directoryIsBeingPreserved = true;
                directoryIsBeingDeleted = false;
                directoryBeingMonitored = filename + @"\";
            }
            else if (destIsReparseDir && !overwriteReparse)
            {
                fdisp.desiredOutcome = Outcome.PreserveExtra;
            }
            else if (destIsNormalFile && !overwriteFile)
            {
                fdisp.desiredOutcome = Outcome.PreserveExtra;
            }
            else if (destIsReparseFile && !overwriteReparse)
            {
                fdisp.desiredOutcome = Outcome.PreserveExtra;
            }

            else // so we have source and destination which are different, and we need to copy the source to the destination
            // But we need to categorise all the different cases so we can report and act correctly
            {
                if (destIsReparseDir)
                {
                    fdisp.desiredOutcome = isNormalDir ? Outcome.ReplaceDirectorySymlinkWithDirectory
                        : isReparseFile ? Outcome.ReplaceDirectorySymlinkWithFileSymlink
                        : isReparseDir ? Outcome.ReplaceDirectorySymlinkWithDirectorySymlink
                        : useHardLinks ? Outcome.ReplaceDirectorySymlinkWithHardLinkToSource : Outcome.ReplaceDirectorySymlinkWithFile;
                }
                else if (destIsReparseFile)
                {
                    fdisp.desiredOutcome = isNormalDir ? Outcome.ReplaceFileOrSymlinkFileWithDirectory
                       : isReparseFile ? Outcome.ReplaceFileOrSymlinkFileWithFileSymlink
                       : isReparseDir ? Outcome.ReplaceFileOrSymlinkFileWithDirectorySymlink
                       : useHardLinks ? Outcome.ReplaceFileOrSymlinkFileWithHardLinkToSource : Outcome.ReplaceFileOrSymlinkFileWithFile;
                }
                else if (destIsNormalDir)
                {

                    fdisp.desiredOutcome = isNormalDir ? Outcome.ReplaceDirectoryWithDirectory
                        : isReparseFile ? Outcome.ReplaceDirectoryWithFileSymlink
                        : isReparseDir ? Outcome.ReplaceDirectoryWithDirectorySymlink
                        : useHardLinks ? Outcome.ReplaceDirectoryWithHardLinkToSource : Outcome.ReplaceDirectoryWithFile;
                    directoryIsBeingDeleted = true;
                    directoryIsBeingPreserved = false;
                    directoryBeingMonitored = filename + @"\";
                }
                else
                {
                    fdisp.desiredOutcome = isNormalDir ? Outcome.ReplaceFileOrSymlinkFileWithDirectory
                       : isReparseFile ? Outcome.ReplaceFileOrSymlinkFileWithFileSymlink
                       : isReparseDir ? Outcome.ReplaceFileOrSymlinkFileWithDirectorySymlink
                       : useHardLinks ? Outcome.ReplaceFileOrSymlinkFileWithHardLinkToSource : Outcome.ReplaceFileOrSymlinkFileWithFile;
                }
            }
        }

        // Finally, we need to deal with the possibility that we need to restore hardlinks in the destination to mirror those in the source.
        // This potentially involves API calls, which is why we defer them to the end, since if we can avoid doing them unnecessarily, we do.

        if (!restoreHardLinks || !isNormalFile) // only normal files are of interest and only if we are restoring hard links
        {
            return;
        }
        switch (fdisp.desiredOutcome)
        {
            case Outcome.NewFile:
            case Outcome.ReplaceFileOrSymlinkFileWithFile:
            case Outcome.ReplaceDirectoryWithFile:
            case Outcome.ReplaceDirectorySymlinkWithFile:

                String hardLinkName = sourcePathAfterDrive + filename; // This is what the hardlink system will find I think
                if (hardlinkInfo.ContainsKey(hardLinkName)) // we're in luck - this file is a hard link we've already found, so use it
                {
                    String alreadyLinkedFile = (String)hardlinkInfo[hardLinkName];

                    switch (fdisp.desiredOutcome)
                    {
                        case Outcome.NewFile:
                            fdisp.desiredOutcome = Outcome.NewHardlinkInDestination; break;
                        case Outcome.ReplaceFileOrSymlinkFileWithFile:
                            fdisp.desiredOutcome = Outcome.ReplaceFileOrSymlinkFileWithHardLinkInDestination; break;
                        case Outcome.ReplaceDirectoryWithFile:
                            fdisp.desiredOutcome = Outcome.ReplaceDirectoryWithHardLinkInDestination; break;
                        case Outcome.ReplaceDirectorySymlinkWithFile:
                            fdisp.desiredOutcome = Outcome.ReplaceDirectorySymlinkWithHardLinkInDestination;
                            break;
                    }

                }
                else
                {
                    StringBuilder s = new StringBuilder(2048);
                    uint len = 2048;
                    IntPtr ffptr = FileFind.FindFirstFileNameW(/*AUP*/(sourcePath + filename), 0, ref len, s);
                    if (ffptr != W32File.INVALID_HANDLE_VALUE)
                    {
                        do
                        {
                            hardLinkName = s.ToString();
                            if (hardLinkName.StartsWith(sourcePathAfterDrive) && (hardLinkName != (sourcePathAfterDrive + filename)))
                            {
                                hardlinkInfo[hardLinkName] = /*AUP*/(newPath + filename);
                                Console.Error.WriteLine("Link name:{0} so can link {1} to {2}", s, hardLinkName, hardlinkInfo[hardLinkName]);
                            }
                            len = 2048;
                            if (!FileFind.FindNextFileNameW(ffptr, ref len, s))
                            {
                                break;
                            }
                        } while (true);
                        FileFind.FindClose(ffptr);
                    } // end if 
                } // end if 
                break;
        } // end case

        // Now we have a specific special case to deal with
        // If the target is a directory and it is being preserved for whatever reason, we need its contents to be preserved regardless
        // Similarly, if the target is a directory and it is being deleted, we need its contents to be deleted too

    }
    public void CloneDelete(FileDisposition fdisp)
    {
        String filename = fdisp.pathName;
        if (filename == "" && (drivePart(newPath) == newPath))
        {
            fdisp.actualOutcome = Outcome.PreserveExtra;
        }
        int i = deletionHelper.DeletePathAndContentsRegardless(newPath + filename); // Should be no contents, but if there are, this will sort it
        if (i < 1)
        {
            fdisp.actualOutcome = Outcome.Failed;
            fdisp.exception = new Win32Exception();
        }
        else
        {
            switch (fdisp.desiredOutcome)
            {
                case Outcome.DeleteDirectoryWithinReplacedDirectory:
                case Outcome.DeleteExtraDirectory:
                case Outcome.DeleteExtraFile:
                case Outcome.DeleteFileWithinReplacedDirectory:
                    fdisp.actualOutcome = fdisp.desiredOutcome;
                    break;
            }

        }
        deletionHelper.DeletePathAndContentsRegardless(/*AUP*/(newPath + filename + bkFileSuffix)); // whenever we delete a target file we must delete the bkfd if it exists
    }
    public void CloneMkdir(FileDisposition fdisp)
    {
        String filename = fdisp.pathName;
        if (filename == "" && (drivePart(newPath) == newPath))
        {
            fdisp.actualOutcome = Outcome.PreserveExtra;
        }
        if (!W32File.CreateDirectory(/*AUP*/(newPath + filename), IntPtr.Zero))
        {
            fdisp.actualOutcome = Outcome.Failed;
            fdisp.exception = new Win32Exception();
        }
        else
        {
            fdisp.actualOutcome = fdisp.desiredOutcome;
        }
    }
    public void openSourceFiles(String sourceFilename, out IntPtr fromFd, out IntPtr fromBkFd)
    {
        fromFd = W32File.INVALID_HANDLE_VALUE;
        fromBkFd = W32File.INVALID_HANDLE_VALUE;

        fromFd = W32File.CreateFile(/*AUP*/(sourcePath + sourceFilename)
     , W32File.EFileAccess.GenericRead
     , W32File.EFileShare.Read
     , IntPtr.Zero
     , W32File.ECreationDisposition.OpenExisting
     , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
     , IntPtr.Zero);
        if (fromFd == W32File.INVALID_HANDLE_VALUE)
        {
            return;
        }
        /* next - see if we have a .bkfd file to go with this file */
        if (cloneAlsoUsesBkf) // otherwise we would just ignore any .bkfd files 
        {
            fromBkFd = W32File.CreateFile(/*AUP*/(sourcePath + sourceFilename) + bkFileSuffix
            , W32File.EFileAccess.GenericRead
            , W32File.EFileShare.Read
            , IntPtr.Zero
            , W32File.ECreationDisposition.OpenExisting
            , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
            , IntPtr.Zero);
            // this can fail - that just means there's no existing bkfd file, which is fine
        }
    }
    public void openDestFile(String newFilename, out IntPtr toFd)
    {
        toFd = W32File.INVALID_HANDLE_VALUE;

        W32File.DeleteFile(/*AUP*/(newPath + newFilename)); // we delete the file first in case it's hardlinked somewhere else
        toFd = W32File.CreateFile(/*AUP*/(newPath + newFilename)
               , W32File.EFileAccess.GenericWrite | W32File.EFileAccess.WriteOwner | W32File.EFileAccess.WriteDAC
               , W32File.EFileShare.Read | W32File.EFileShare.Write | W32File.EFileShare.Delete
               , IntPtr.Zero
               , W32File.ECreationDisposition.OpenAlways
               , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
               , IntPtr.Zero);
    }
    public void openDestBkFile(String newFilename, out IntPtr toBkFd)
    {
        toBkFd = W32File.INVALID_HANDLE_VALUE;
        if (createBkf) // otherwise we would just ignore any .bkfd files 
        {
            toBkFd = W32File.CreateFile(/*AUP*/(newPath + newFilename) + bkFileSuffix
            , W32File.EFileAccess.GenericWrite | W32File.EFileAccess.WriteOwner | W32File.EFileAccess.WriteDAC
            , W32File.EFileShare.Read | W32File.EFileShare.Write | W32File.EFileShare.Delete
            , IntPtr.Zero
            , W32File.ECreationDisposition.OpenAlways
            , 0
            , IntPtr.Zero);
        }
    }
    public void TransferData(FileDisposition fdisp, IntPtr fromFd, IntPtr fromBkFd, IntPtr toFd, IntPtr toBkFd)
    {
        byte[] md5buffer = new byte[1024 * 1034];
        IntPtr wcontext = IntPtr.Zero;
        BackupReader b = null;

        Boolean needToWriteFile = toFd != W32File.INVALID_HANDLE_VALUE;
        Boolean needToWriteBackupFile = toBkFd != W32File.INVALID_HANDLE_VALUE;
        W32File.BY_HANDLE_FILE_INFORMATION fromFileInformation = fdisp.fromFileInfo.w32fileinfo;
        try
        {
            uint bytesWritten = 0;
            b = new BackupReader(fromFd, fromBkFd);
            Boolean currentStreamGoesToOutBkFd = false;
            Boolean currentStreamGoesToOutFd = false;
            Boolean currentStreamGoesToChecksum = false;
            Boolean alreadySeenSecurityData = false;
            Boolean alreadyWrittenGreenwheelHeader = false;
            using (MD5 md5 = MD5.Create())
            {
                md5.Initialize();
                do
                {
                    // if we have a fromBkFd and a fromFd we merge the two to create a stream
                    // if we only have one, we use it directly
                    // all this is encapsulated in b
                    // The end result should be that we get a sequence of stream headers and stream data in exactly the right format for a BackupWrite to restore
                    // the file we want. We can then pick and choose individual streams to go to the outBkFd if we are producing one.
                    // It is an open question whether backup write cares about the precise order of the streams but we play safe. 

                    // We now also calculate checksums on the fly for future use

                    if (!b.readNextStreamPart())
                    {
                        break;
                    }
                    if (b.atNewStreamHeader)
                    {
                        // Console.WriteLine("Stream Header: Size {0} Type {1} Name {2}", b.streamHeader.Size, b.streamHeader.StreamId, b.currentStreamName);

                        if (b.streamHeader.StreamId == (uint)W32File.StreamIdValue.BACKUP_GREENWHEEL_HEADER)
                        {
                            currentStreamGoesToOutBkFd = needToWriteBackupFile;
                            currentStreamGoesToOutFd = false;
                            fromFileInformation.FileAttributes = (FileAttributes)b.streamHeader.StreamAttributes;
                            alreadyWrittenGreenwheelHeader = true;
                        }
                        else
                        {
                            if (needToWriteBackupFile && !alreadyWrittenGreenwheelHeader)
                            {
                                W32File.WIN32_STREAM_ID greenStream = new W32File.WIN32_STREAM_ID();
                                greenStream.StreamId = (uint)W32File.StreamIdValue.BACKUP_GREENWHEEL_HEADER;
                                greenStream.Size = 236; // big enough
                                greenStream.StreamNameSize = 0;
                                greenStream.StreamNameData = IntPtr.Zero;
                                greenStream.StreamAttributes = (uint)fromFileInformation.FileAttributes;

                                uint nbw = 0;
                                int greenStreamSize = 0;
                                IntPtr greenStreamPtr = Marshal.AllocHGlobal((int)greenStream.Size + W32File.MIN_WIN32_STREAM_ID_SIZE);
                                IntPtr greenStreamPtrAt = greenStreamPtr;
                                Marshal.StructureToPtr(greenStream, greenStreamPtrAt, false);
                                greenStreamSize += W32File.MIN_WIN32_STREAM_ID_SIZE;
                                greenStreamSize += CHECKSUM_SIZE;
                                greenStreamPtrAt = new IntPtr(greenStreamPtr.ToInt64() + (Int64)greenStreamSize);
                                Marshal.StructureToPtr(fromFileInformation, greenStreamPtrAt, false);
                                greenStreamSize += Marshal.SizeOf(fromFileInformation);
                                greenStreamPtrAt = new IntPtr(greenStreamPtr.ToInt64() + (Int64)greenStreamSize);

                                bool ok = W32File.WriteFile(toBkFd, greenStreamPtr, (uint)(W32File.MIN_WIN32_STREAM_ID_SIZE + greenStream.Size), out nbw, IntPtr.Zero);
                                Marshal.FreeHGlobal(greenStreamPtr);
                            }
                            alreadyWrittenGreenwheelHeader = true;

                            switch (b.streamHeader.StreamId)
                            {
                                case (uint)W32File.StreamIdValue.BACKUP_SECURITY_DATA:
                                    {
                                        if (alreadySeenSecurityData)
                                        {
                                            currentStreamGoesToOutBkFd = false;
                                            currentStreamGoesToOutFd = false;
                                        }
                                        else
                                        {
                                            alreadySeenSecurityData = true;
                                            currentStreamGoesToOutFd = needToWriteFile && restorePermissions;
                                            currentStreamGoesToOutBkFd = needToWriteBackupFile; // permissions are always written to a bkf if they are found
                                        }
                                        break;
                                    }
                                case (uint)W32File.StreamIdValue.BACKUP_DATA:
                                    {
                                        currentStreamGoesToOutFd = needToWriteFile && restoreFileContents;
                                        currentStreamGoesToOutBkFd = needToWriteBackupFile && createBkfData;
                                        currentStreamGoesToChecksum = true;
                                        break;
                                    }
                                case (uint)W32File.StreamIdValue.BACKUP_EA_DATA:
                                    {
                                        currentStreamGoesToOutFd = needToWriteFile && restoreAFS;
                                        currentStreamGoesToOutBkFd = needToWriteBackupFile && createBkfAFS;
                                        currentStreamGoesToChecksum = false;
                                        break;
                                    }
                                default:
                                    {
                                        currentStreamGoesToOutFd = needToWriteFile;
                                        currentStreamGoesToOutBkFd = needToWriteBackupFile;

                                        break;
                                    }
                            }
                        }
                    }
                    if (currentStreamGoesToOutBkFd)
                    {
                        uint nbw = 0;
                        bool ok = W32File.WriteFile(toBkFd, b.streamDataSegmentStart, b.streamDataSegmentSize, out nbw, IntPtr.Zero);
                    }
                    if (currentStreamGoesToOutFd)
                    {
                        if (!W32File.BackupWrite(toFd,
                                     b.streamDataSegmentStart,
                                     b.streamDataSegmentSize,
                                     out bytesWritten,
                                     false,
                                     restorePermissions,
                                     ref wcontext))
                        {
                            fdisp.exception = new Win32Exception();
                            break;
                        }
                        if (b.streamDataSegmentSize != bytesWritten)
                        {
                            fdisp.exception = new Win32Exception(); break;
                        }

                    }
                    if (currentStreamGoesToChecksum && !b.atNewStreamHeader)
                    {
                        Marshal.Copy(b.streamDataSegmentStart, md5buffer, 0, (int)b.streamDataSegmentSize);
                        md5.TransformBlock(md5buffer, 0, (int)b.streamDataSegmentSize, null, 0);
                    }
                }
                while (true);
                md5.TransformFinalBlock(md5buffer, 0, 0);
                // now we rewind the backup file and write the checksum
                if (needToWriteBackupFile)
                {
                    long newPos = 0;
                    W32File.SetFilePointerEx(toBkFd, (long)(W32File.MIN_WIN32_STREAM_ID_SIZE), out newPos, 0);
                    if (newPos == W32File.MIN_WIN32_STREAM_ID_SIZE)
                    {
                        IntPtr pBufferLocal = Marshal.AllocHGlobal(1024);
                        int hashSize = md5.HashSize / 8;

                        Marshal.Copy(md5.Hash, 0, pBufferLocal, hashSize);
                        W32File.WriteFile(toBkFd, pBufferLocal, (uint)hashSize, out bytesWritten, IntPtr.Zero);
                        Marshal.FreeHGlobal(pBufferLocal);
                    }
                }
                // Console.WriteLine(BitConverter.ToString(md5.Hash));
            }
        }
        finally
        {
            uint dummy;
            if (fdisp.exception == null)
            {
                if (wcontext != IntPtr.Zero)
                {
                    if (!W32File.BackupWrite(IntPtr.Zero,
                                 IntPtr.Zero,
                                 0,
                                 out dummy,
                                 true,
                                 restorePermissions,
                                 ref wcontext))
                    {
                        fdisp.exception = new Win32Exception();
                    };
                }
                if (b != null && b.rcontext != IntPtr.Zero)
                {
                    if (!W32File.BackupRead(IntPtr.Zero,
                                IntPtr.Zero,
                                0,
                                out dummy,
                                true,
                                restorePermissions,
                                ref b.rcontext))
                    {
                        fdisp.exception = new Win32Exception();
                    };
                }
            }

            Boolean setFileAttribsResult;
            if (fdisp.exception == null)
            {
                if (needToWriteFile)
                {
                    W32File.SetFileTime(toFd, ref fromFileInformation.CreationTime, ref fromFileInformation.LastAccessTime, ref fromFileInformation.LastWriteTime);
                    if (restoreAttributes)
                    {
                        FileAttributes mask = FileAttributes.ReadOnly | FileAttributes.Hidden | FileAttributes.System | FileAttributes.Normal | FileAttributes.Archive;
                        FileAttributes attribsToSet = fromFileInformation.FileAttributes & mask;
                        setFileAttribsResult = W32File.SetFileAttributes(/*AUP*/(newPath + fdisp.pathName), attribsToSet);

                    }
                };
                if (needToWriteBackupFile)
                {
                    W32File.SetFileTime(toBkFd, ref fromFileInformation.CreationTime, ref fromFileInformation.LastAccessTime, ref fromFileInformation.LastWriteTime);
                    // this allows us to tell if the bkfd is in sync with its companion file
                };
            }

            if (fdisp.exception == null)
            {
                fdisp.actualOutcome = fdisp.desiredOutcome;
            }
            else
            {
                fdisp.actualOutcome = Outcome.Failed;
            }

        }
    }

    public void CloneAct(FileDisposition fdisp)
    {
        String filename = fdisp.pathName;
        IntPtr fromFd = W32File.INVALID_HANDLE_VALUE, fromBkFd = W32File.INVALID_HANDLE_VALUE, toFd = W32File.INVALID_HANDLE_VALUE, toBkFd = W32File.INVALID_HANDLE_VALUE;

        Boolean transferNeeded = false;
        Boolean bkfTransferNeeded = false;


        switch (fdisp.desiredOutcome)
        {
            case Outcome.DeleteExtraDirectory:
            case Outcome.DeleteExtraFile:
                // Do this later.....
                break;

            case Outcome.NewDirectory:
            case Outcome.ReplaceDirectorySymlinkWithDirectory:
            case Outcome.ReplaceDirectoryWithDirectory:
            case Outcome.ReplaceFileOrSymlinkFileWithDirectory:
                CloneMkdir(fdisp);
                transferNeeded = true;
                fdisp.actualOutcome = fdisp.desiredOutcome;
                bkfTransferNeeded = createBkf;
                break;
            case Outcome.NewDirectorySymlink:
            case Outcome.ReplaceDirectoryWithDirectorySymlink:
            case Outcome.ReplaceDirectorySymlinkWithDirectorySymlink:
            case Outcome.ReplaceFileOrSymlinkFileWithDirectorySymlink:
                CloneMkdir(fdisp);
                transferNeeded = true;
                fdisp.actualOutcome = fdisp.desiredOutcome;
                bkfTransferNeeded = createBkf; break;
            case Outcome.NewFileSymlink:
            case Outcome.ReplaceDirectoryWithFileSymlink:
            case Outcome.ReplaceFileOrSymlinkFileWithFileSymlink:
            case Outcome.ReplaceDirectorySymlinkWithFileSymlink:
                transferNeeded = true;
                fdisp.actualOutcome = fdisp.desiredOutcome;
                bkfTransferNeeded = createBkf;
                break;
            case Outcome.ReplaceDirectorySymlinkWithFile:
            case Outcome.ReplaceDirectoryWithFile:
            case Outcome.NewFile: //XXX check
                transferNeeded = true;
                fdisp.actualOutcome = fdisp.desiredOutcome;
                bkfTransferNeeded = createBkf;
                break;
            case Outcome.ReplaceFileOrSymlinkFileWithFile:
                // The replace with file scenarios are the ones where we deferred deleting the source in case we can do something clever later on.
                deletionHelper.DeletePathAndContentsRegardless(/*AUP*/(newPath + filename));
                deletionHelper.DeletePathAndContentsRegardless(/*AUP*/(newPath + filename + bkFileSuffix));
                transferNeeded = true;
                fdisp.actualOutcome = fdisp.desiredOutcome;
                bkfTransferNeeded = createBkf;
                break;
            case Outcome.NewHardlinkToSource:
            case Outcome.ReplaceDirectorySymlinkWithHardLinkToSource:
            case Outcome.ReplaceDirectoryWithHardLinkToSource:
            case Outcome.ReplaceFileOrSymlinkFileWithHardLinkToSource:
                {
                    Boolean linkOK = W32File.CreateHardLink(/*AUP*/(newPath + filename), /*AUP*/(sourcePath + filename), IntPtr.Zero);
                    if (!linkOK)
                    {
                        fdisp.actualOutcome = Outcome.Failed;
                        fdisp.exception = new Win32Exception();
                    }
                    else
                    {
                        fdisp.actualOutcome = fdisp.desiredOutcome;
                    }
                }
                break;
            case Outcome.NewHardlinkInDestination:
            case Outcome.ReplaceDirectoryWithHardLinkInDestination:
            case Outcome.ReplaceDirectorySymlinkWithHardLinkInDestination:
            case Outcome.ReplaceFileOrSymlinkFileWithHardLinkInDestination:
                {
                    Boolean linkOK = false;
                    Boolean bkLinkOK = true;
                    String hardLinkName = sourcePathAfterDrive + filename; // This is what the hardlink system will find I think
                    if (hardlinkInfo.ContainsKey(hardLinkName)) // we're in luck - this file is a hard link we've already found, so use it
                    {
                        String alreadyLinkedFile = (String)hardlinkInfo[hardLinkName];
                        linkOK = W32File.CreateHardLink(/*AUP*/(newPath + filename), /*AUP*/(alreadyLinkedFile), IntPtr.Zero);
                        if (!linkOK)
                        {
                            fdisp.actualOutcome = Outcome.Failed;
                            Console.Error.WriteLine("Internal hard link failed from {0} to {1}", filename, alreadyLinkedFile);
                            fdisp.exception = new Win32Exception();
                        }
                        else
                        {
                            if (createBkf) bkLinkOK = W32File.CreateHardLink(/*AUP*/(newPath + filename) + bkFileSuffix, alreadyLinkedFile + bkFileSuffix, IntPtr.Zero);

                            if (!bkLinkOK)
                            {
                                fdisp.actualOutcome = Outcome.Failed;
                                fdisp.exception = new Win32Exception();
                            }
                            else
                            {
                                fdisp.actualOutcome = fdisp.desiredOutcome;
                            }
                        }
                    }
                }
                break;

            case Outcome.OnlyNeedBkFile: // The main file was already OK, but we are missing a bkfd file
                fdisp.actualOutcome = fdisp.desiredOutcome;
                transferNeeded = false;
                bkfTransferNeeded = createBkf;
                break;

            case Outcome.IgnoredSource:
            case Outcome.Unchanged:
            case Outcome.PreserveExtra:
            case Outcome.Failed:
            case Outcome.FailedAndBroke:
            case Outcome.DeleteDirectoryWithinReplacedDirectory:
            case Outcome.DeleteFileWithinReplacedDirectory:

                // Nothing to do 
                fdisp.actualOutcome = fdisp.desiredOutcome;
                break;
            default:
                // Shouldn't happen......
                break;
        }
        if (transferNeeded || bkfTransferNeeded)
        {
            try
            {
                openSourceFiles(filename, out fromFd, out fromBkFd);
                if (fromFd == W32File.INVALID_HANDLE_VALUE)
                {
                    fdisp.actualOutcome = Outcome.Failed;
                    fdisp.exception = new Win32Exception();
                    return;
                }
                if (transferNeeded)
                {
                    openDestFile(filename, out toFd);
                    if (toFd == W32File.INVALID_HANDLE_VALUE)
                    {
                        fdisp.actualOutcome = Outcome.Failed;
                        fdisp.exception = new Win32Exception();
                        return;
                    }
                }
                if (bkfTransferNeeded)
                {
                    openDestBkFile(filename, out toBkFd);
                    if (toBkFd == W32File.INVALID_HANDLE_VALUE)
                    {
                        fdisp.actualOutcome = Outcome.Failed;
                        fdisp.exception = new Win32Exception();
                        return;
                    }
                }
                TransferData(fdisp, fromFd, fromBkFd, toFd, toBkFd);
            }
            finally
            {
                if (fromFd != W32File.INVALID_HANDLE_VALUE) W32File.CloseHandle(fromFd);
                if (fromBkFd != W32File.INVALID_HANDLE_VALUE) W32File.CloseHandle(fromBkFd);
                if (toFd != W32File.INVALID_HANDLE_VALUE) W32File.CloseHandle(toFd);
                if (toBkFd != W32File.INVALID_HANDLE_VALUE) W32File.CloseHandle(toBkFd);
            }
        }

    }
}