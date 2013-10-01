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
using System.Text;
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
        if (isKept)
        {
            Console.WriteLine("                  :KEEP:{0}", filename);
            return;
        }
        if (isDir)
        {
            Console.WriteLine("                  :DELD:{0}", filename);
        }
        else
        {
            Console.WriteLine("                  :DELF:{0}", filename);
        }
    }
    public int DeletePathAndContentsRegardless(string foundFileName)
    {
        return DeletePathAndContentsUnless(foundFileName, emptyHashtable);
    }
    public int DeletePathAndContentsUnless(string foundFileName, Hashtable filesToKeep)
    {
        // this no nonsense method will do its damndest to delete a path regardless of what it is or what ( if a directory ) it contains.
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
            //Console.WriteLine("Read {0}", bytesRead);
        }
        // so we have some data in a buffer - either cos we've just read it or cos it was left over from last time
        // are we at the start of a brand new stream?
        if (atLastSegmentOfCurrentStream || atStartOfFile)
        {
            // at this point, we are either at the end of the file, or we are at the start of the stream
            // if we are unlucky, we have some but not all of the next stream header. So to prevent any problems, we check for this 
            atStartOfFile = false;
            atLastSegmentOfCurrentStream = false;
            //Console.WriteLine("CHECKING:{0} in buffer", pBufferSize - pBufferPos);
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
    NewTarget = 1,
    NewDirectoryTarget = 5,
    NewHardlinkTarget = 11,

    ReplaceTarget = 31,
    ReplaceTargetWithHardlink = 32,
    ReplaceTargetWithDirectory = 33,
    ReplaceTargetDirectory = 34,
    ReplaceTargetDirectoryWithHardLink = 35,

    PreserveTarget = 41,
    DeleteTarget = 42,

    Unchanged = 51,
    OnlyNeedBkFile = 52,
    // bad outcomes, failed means failed and did nothing, failedAndBroke means did something but not the right thing!
    Failed = 99,
    FailedAndBroke = 100
}
public class FileDisposition
{
    public String pathName = null;
    public FileInfo fromFileInfo = null;
    public FileInfo toFileInfo = null;
    public Outcome desiredOutcome;
    public Outcome actualOutcome;
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
    public Boolean cloneAlsoUsesBkf = false; // if true, we look for extra information and use this in addition to the original file ( if any )

    // What to produce on the other side
    public Boolean restoreFileContents = true; // if true, we create an output file with the same contents as the input file
    public Boolean restoreAFS = true; // if true, the output file will also contain any AFS ( named streams ) in the input. Destination must be NTFS
    public Boolean restorePermissions = false; // if true, the output file will have the identical permissions to the input file. This can have unintended consequences.
    public Boolean restoreAttributes = true; // if true, the output file will recreate attributes such as readonly, system, hidden
    public Boolean restoreTimes = true; // if true, the output file will have the last mod time etc copied from the source
    public Boolean restoreHardLinks = true; // if true we notice when source files are hard links within the hierarchy being copied, 
    // and if so we restore them as hardlinks on the other side


    public Boolean createBkf = false; // if true, all data apart from file contents is written to an extra location to allow a restore to recreate the original even if the destination
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
    public String originalPath;
    public String newPath;
    public String bkFileSuffix = ".bkfd";


    public Hashtable filesToKeep = new Hashtable();
    public Dictionary<String, FileInfo> fromFileList = new Dictionary<string, FileInfo>(500000), toFileList = new Dictionary<string, FileInfo>(500000);
    public Dictionary<String, FileDisposition> actionList = new Dictionary<string, FileDisposition>(500000);

    public Outcome outcome;
    public ReportStage reportStage;
    public int nFiles = 0;
    public int nDirs = 0;
    public int nSpecial = 0;
    public int nFailed = 0;
    public int nSame = 0;
    public int nIgnored = 0;
    public int nExcluded = 0;
    public int nOverwritten = 0;
    public int nDeleted = 0;
    public int nInternalHardLinked = 0;
    private String unicodePrefix = @"\\?\";
    private int originalPathLength;
    // store hard link information as needed
    private Hashtable hardlinkInfo = new Hashtable();
    public const int CHECKSUM_SIZE = 32;
    public DeletionHelper deletionHelper = new DeletionHelper(); // public so we can override the reporting if we wish

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
    public String prettyPrintOutcome(Outcome finalOutcome)
    {
        String didWhat;
        switch (finalOutcome)
        {
            case Outcome.PreserveTarget:
                didWhat = "PRSV"; break;
            case Outcome.IgnoredSource:
                didWhat = "SKIP"; break;
            case Outcome.NewHardlinkTarget:
                didWhat = "NHLK"; break;
            case Outcome.Unchanged:
                didWhat = "SAME"; break;
            case Outcome.OnlyNeedBkFile:
                didWhat = "SAMK"; break;
            case Outcome.NewTarget:
            case Outcome.NewDirectoryTarget:
                didWhat = "NEW "; break;
            case Outcome.NotFinished:
                didWhat = "ABRT"; break;
            case Outcome.ReplaceTarget:
            case Outcome.ReplaceTargetDirectory:
            case Outcome.ReplaceTargetWithDirectory:
                didWhat = "OVWR"; break;
            case Outcome.ReplaceTargetWithHardlink:
            case Outcome.ReplaceTargetDirectoryWithHardLink:
                didWhat = "RHLK"; break;
            case Outcome.DeleteTarget:
                didWhat = "DELE"; break;
            default:
                didWhat = "????";
                break;
        }
        return didWhat;
    }

    protected virtual Boolean exclude(String fromFilename)
    {
        // the default excluder - can be overriden in derived classes
        // return true if this file should not be copied based on its name only
        if (excludeList == null) return false;
        if (fromFilename.Length <= originalPathLength) return false;
        String pathToMatch = @"\" + fromFilename.Substring(originalPathLength) + @"\";
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
        // and always ends in \ even if the path isn't a directory.
        StringBuilder newFromPath = new StringBuilder(fromPath.Length);
        Boolean lastWasBackslash = false;
        Boolean atFirstChar = true;
        foreach (Char c in fromPath)
        {
            if (atFirstChar)
            {
                atFirstChar = false;
                newFromPath.Append(c);
                continue;
            }
            if (c == '\\')
            {
                if (lastWasBackslash)
                {
                    continue;
                }
                lastWasBackslash = true;
            }
            else
            {
                lastWasBackslash = false;
            }
            newFromPath.Append(c);
        }
        // if the very last character was a backslash, eliminate it if this isn't the root of a drive expressed in one of several forms:
        // X:\
        // \\?\Volume{......}\
        // \\?\GLOBAL....ShadowCopynn\
        return newFromPath.ToString();
    }
    public Backup(String of, String nf)
    {
        originalPath = of;
        newPath = nf;
        char[] backslashes = { '\\' };
        // Because we support long filenames, we use windows functions which don't understand forward slashes. But because we do support forward slashes, we convert them here
        originalPath = originalPath.Replace(@"/", @"\");
        newPath = newPath.Replace(@"/", @"\");
        originalPath = fixUpPath(originalPath);
        newPath = fixUpPath(newPath);
        originalPathLength = of.Length;
    }

    public static int reportVerbosity = 1;

    // override this in a derived class if you want to report differently
    protected virtual void reporter(String filename, FileDisposition disposition)
    {
        if (reportVerbosity == 0) return;
    }

    // public delegate void reportFunc(String originalFilename, String newFilename, Boolean isDir, Boolean isSpecial, long filelen, long donesofar, reportStage stage, outcome finalOutcome, Win32Exception e);
    // public reportFunc reporter;

    public delegate Boolean cloneFunc(String originalFilename, FileFind.WIN32_FIND_DATA fdata, Dictionary<String, FileInfo> fileList);

    virtual public void doit()
    {
        if (excludeList != null)
        {
            excludeList.Sort();
        }
        FileFind.WIN32_FIND_DATA fdata = new FileFind.WIN32_FIND_DATA();
        fdata.dwFileAttributes = (int)FileAttributes.Directory;
        // first a sanity check - create newPath as a directory in case it doesn't exist yet
        W32File.CreateDirectory(addUnicodePrefix(newPath), IntPtr.Zero); // if it fails so what.....
        CloneRecursive(originalPath, "", fdata, fromFileList, CloneGetSourceFiles);

        CloneRecursive(newPath, "", fdata, toFileList, CloneGetDestFiles);
        Console.WriteLine("SourceFileList contains {0} entries", fromFileList.Count);
        Console.WriteLine("TargetFileList contains {0} entries", toFileList.Count);
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
                    actionList[mainFn].toFileInfo.hasBkfd = true;
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
        foreach (KeyValuePair<String, FileDisposition> action in actionList)
        {
            CloneDecide(action.Value);
            switch (action.Value.desiredOutcome)
            {
                case Outcome.DeleteTarget: toDelete.Add(action.Value); break;
                case Outcome.NewTarget:
                case Outcome.NewDirectoryTarget:
                    if (action.Value.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory))
                    {
                        toMkdir.Add(action.Value); // this takes care of the possibility that the target is a junction or directory symlink
                    }
                    break;
                case Outcome.ReplaceTargetDirectory:
                case Outcome.ReplaceTarget:
                    toDelete.Add(action.Value);
                    if (action.Value.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory))
                    {
                        toMkdir.Add(action.Value); // this takes care of the possibility that the target is a junction or directory symlink
                    }
                    break;
                case Outcome.ReplaceTargetWithDirectory: toDelete.Add(action.Value); toMkdir.Add(action.Value); break;

                case Outcome.ReplaceTargetDirectoryWithHardLink: toDelete.Add(action.Value); break;

            }
        }
        // So now we know what we want to do
        // Let's work out a sensible order to do it in
        // First delete things which shouldn't be there, but preserve normal files which are changing just in case we can use the old contents
        // While we delete, we need to delete backwards, so we get rid of contents before directories containing them....
        // Then create any needed directories
        // Finally, copy over files
        // 

        int nDeletions = toDelete.Count;
        for (int i = nDeletions - 1; i >= 0; i--)
        {
            CloneDelete(toDelete[i]);
        }
        int nMkdir = toMkdir.Count;
        for (int i = 0; i < nMkdir; i++)
        {
            CloneMkdir(toMkdir[i]);
        }
        // Finally, do the rest of the work
        foreach (KeyValuePair<String, FileDisposition> action in actionList)
        {
            CloneAct(action.Value);
        }

        int nLateDeletions = toDeleteAtEnd.Count;
        for (int i = nLateDeletions - 1; i >= 0; i--)
        {
            CloneDelete(toDeleteAtEnd[i]);
        }
        return;
        /*
              if (recursive)
              {
                  CloneRecursive(originalPath, "", fdata, null, CloneOne);
              }
              else
              {
                  CloneOne("", null);
              }
              if (removeExtra && (nFailed == 0))
              {
                  nDeleted = deletionHelper.DeleteDirectoryContentsRecursivelyUnless(newPath, filesToKeep);
              }
         * */
    }
    static ulong dsistart = 0;
    public void displayStreamInfo(String originalFilename, IntPtr pBuffer, long sofar, long bytesRead)
    {

        if (sofar == 0)
        {
            dsistart = 0;
            // this is definitely a stream header
            W32File.WIN32_STREAM_ID wid1;
            wid1 = (W32File.WIN32_STREAM_ID)Marshal.PtrToStructure(pBuffer, typeof(W32File.WIN32_STREAM_ID));
            dsistart = wid1.Size;
            //Console.WriteLine("DSI:{0}:{1}:{2}", wid1.StreamId, wid1.StreamNameSize, wid1.Size);
        }
    }

    public void CloneRecursive(string basePath, string fn1, FileFind.WIN32_FIND_DATA fdata, Dictionary<String, FileInfo> fileList, cloneFunc cloner)
    {
        /* If this is a .bkfd file, then don't clone it! */
        String source = basePath + fn1;
        String dest = basePath + fn1;
        if (source.EndsWith(bkFileSuffix))
        {
            return;
        }
        Boolean shouldRecurse = cloner(fn1, fdata, fileList); // This may throw an exception if the reporter wants to exit if there's an error
        if (!shouldRecurse) return;
        FileFind.WIN32_FIND_DATA fdata1 = new FileFind.WIN32_FIND_DATA();
        IntPtr findhandle;
        findhandle = FileFind.FindFirstFile(source + @"\*", ref fdata1);
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
    public bool CloneGetSourceFiles(String fileName, FileFind.WIN32_FIND_DATA fdata, Dictionary<String, FileInfo> fileList)
    {
        // try out the new version
        FileInfo fid = new FileInfo();

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

        if (fileList.Count % 1000 == 0)
        {
            Console.WriteLine("SourceFileList {0} {1}", fileList.Count, fileName);
        }

        return fid.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory) && !
            fid.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);

        Boolean isNormalDir = false;

        IntPtr fd = W32File.INVALID_HANDLE_VALUE;
        IntPtr bkFd = W32File.INVALID_HANDLE_VALUE;
        FileInfo fi = new FileInfo();
        filesToKeep.Add(newPath + fileName, originalPath + fileName);
        if (createBkf)
        {
            filesToKeep.Add(newPath + fileName + bkFileSuffix, originalPath + fileName);
        }
        try
        {
            if (exclude(fileName))
            {
                fi.excluded = true;
                fi.failedToOpen = true;

                return false;
            }

            fd = W32File.CreateFile(addUnicodePrefix(originalPath + fileName)
            , W32File.EFileAccess.GenericRead
            , W32File.EFileShare.Read
            , IntPtr.Zero
            , W32File.ECreationDisposition.OpenExisting
            , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
            , IntPtr.Zero);
            if (fd == W32File.INVALID_HANDLE_VALUE)
            {
                fi.failedToOpen = true;
                return false;
            }
            fi.failedToOpen = false;

            bkFd = W32File.CreateFile(addUnicodePrefix(originalPath + fileName + bkFileSuffix)
            , W32File.EFileAccess.GenericRead
            , W32File.EFileShare.Read
            , IntPtr.Zero
            , W32File.ECreationDisposition.OpenExisting
            , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
            , IntPtr.Zero);
            fi.hasBkfd = bkFd != W32File.INVALID_HANDLE_VALUE;

            // So the source location exists and we can open it. Good start......
            W32File.GetFileInformationByHandle(fd, out fi.w32fileinfo);
            // Now, was it a directory or reparse
            Boolean dorrd = fi.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory);
            Boolean rp = fi.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);

            isNormalDir = dorrd && !rp;
            return isNormalDir;
        }
        finally
        {
            W32File.CloseHandle(fd);
            W32File.CloseHandle(bkFd);
            fileList.Add(fileName, fi);
            if (fileList.Count % 1000 == 0)
            {
                Console.WriteLine("SourceFileList {0} {1}", fileList.Count, fileName);
            }
        }
        /*
                fileList.Add(fileName, fi);
                if (fileList.Count % 1000 == 0)
                {
                    Console.WriteLine("SourceFileList {0} {1}", fileList.Count, fileName);
                }
                return true;
         * */
    }
    public bool CloneGetDestFiles(String fileName, FileFind.WIN32_FIND_DATA fdata, Dictionary<String, FileInfo> fileList)
    {

        // try out the new version
        FileInfo fid = new FileInfo();

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

        if (fileList.Count % 1000 == 0)
        {
            Console.WriteLine("DestFileList {0} {1}", fileList.Count, fileName);
        }

        return fid.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory) && !
            fid.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);


        /*
                Boolean isNormalDir = false;

                IntPtr fromFd = W32File.INVALID_HANDLE_VALUE;
                IntPtr fromBkFd = W32File.INVALID_HANDLE_VALUE;
                FileInfo fi = new FileInfo();

                try
                {
                    if (exclude(fileName))
                    {
                        fi.excluded = true;
                        fi.failedToOpen = true;

                        return false;
                    }

                    fromFd = W32File.CreateFile(addUnicodePrefix(newPath + fileName)
                    , W32File.EFileAccess.GenericRead
                    , W32File.EFileShare.Read
                    , IntPtr.Zero
                    , W32File.ECreationDisposition.OpenExisting
                    , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
                    , IntPtr.Zero);
                    if (fromFd == W32File.INVALID_HANDLE_VALUE)
                    {
                        fi.failedToOpen = true;
                        return false;
                    }
                    fi.failedToOpen = false;

                    fromBkFd = W32File.CreateFile(addUnicodePrefix(newPath + fileName + bkFileSuffix)
                    , W32File.EFileAccess.GenericRead
                    , W32File.EFileShare.Read
                    , IntPtr.Zero
                    , W32File.ECreationDisposition.OpenExisting
                    , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
                    , IntPtr.Zero);
                    fi.hasBkfd = fromBkFd != W32File.INVALID_HANDLE_VALUE;

                    // So the source location exists and we can open it. Good start......
                    W32File.GetFileInformationByHandle(fromFd, out fi.w32fileinfo);
                    // Now, was it a directory or reparse
                    Boolean dorrd = fi.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory);
                    Boolean rp = fi.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);

                    isNormalDir = dorrd && !rp;
                    return isNormalDir;
                }
                finally
                {
                    W32File.CloseHandle(fromFd);
                    W32File.CloseHandle(fromBkFd);
                    fileList.Add(fileName, fi);
                    if (fileList.Count % 1000 == 0)
                    {
                        Console.WriteLine("DestFileList {0}", fileList.Count);
                    }
                }
        */
    }
    public void CloneDecide(FileDisposition fdisp)
    {
        String filename = fdisp.pathName;
        if (fdisp.fromFileInfo == null)
        {
            // This is an extra destination file
            if (removeExtra && !fdisp.toFileInfo.excluded)
            {
                fdisp.desiredOutcome = Outcome.DeleteTarget; return;
            }
            else
            {
                fdisp.desiredOutcome = Outcome.PreserveTarget; return;
            }
        }
        Boolean isNormalDir;
        Boolean isReparseDir;
        Boolean isReparseFile;
        Boolean isNormalFile;
        Boolean dorrd = fdisp.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory);
        Boolean rp = fdisp.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);
        isNormalDir = dorrd && !rp;
        isReparseDir = dorrd && rp;
        isReparseFile = !dorrd && rp;
        isNormalFile = !dorrd && !rp;
        long flen = (((long)fdisp.fromFileInfo.w32fileinfo.FileSizeHigh) << 32) + fdisp.fromFileInfo.w32fileinfo.FileSizeLow;

        if (fdisp.fromFileInfo.excluded)
        {
            fdisp.desiredOutcome = Outcome.IgnoredSource; return;
        }
        else if (fdisp.toFileInfo == null)
        {
            fdisp.desiredOutcome = isNormalDir ? Outcome.NewDirectoryTarget : Outcome.NewTarget;
        }
        else
        {

            Boolean dDir = fdisp.toFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory);
            Boolean dReparse = fdisp.toFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint);
            Boolean destIsNormalFile = !dDir && !dReparse;
            Boolean destIsNormalDir = dDir && !dReparse;
            Boolean destIsReparseDir = dDir && dReparse;
            Boolean destIsReparseFile = !dDir && dReparse;
            long dFlen = (((long)fdisp.toFileInfo.w32fileinfo.FileSizeHigh) << 32) + fdisp.toFileInfo.w32fileinfo.FileSizeLow;
            // Now see if we're interested in handling this kind of source file
            if (
                (isReparseDir && !cloneReparse) ||
                (isReparseFile && !cloneReparse) ||
                (isNormalDir && !cloneDir) ||
                (isNormalFile && !cloneFile)
                )
            {
                fdisp.desiredOutcome = Outcome.IgnoredSource; return;
            }
            if (
                (rp == dReparse) && (dorrd == dDir)
                )
            {
                // so source and destination are the same type
                if (!overwriteSameType)
                {
                    fdisp.desiredOutcome = Outcome.PreserveTarget; return;
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
                    return;
                }
                else
                {
                    // so the files are the same type but not identical
                    fdisp.desiredOutcome = Outcome.ReplaceTarget;
                }
            }
            // so the destination is not the same type as the source
            if (destIsNormalDir && !overwriteDir)
            {
                fdisp.desiredOutcome = Outcome.PreserveTarget; return;
            }
            else if (destIsNormalDir)
            {
                fdisp.desiredOutcome = Outcome.ReplaceTargetDirectory; return;
            }
            else if (destIsReparseDir && !overwriteReparse)
            {
                fdisp.desiredOutcome = Outcome.PreserveTarget; return;
            }
            else if (destIsReparseDir)
            {
                fdisp.desiredOutcome = Outcome.ReplaceTargetDirectory; return;
            }
            else if (destIsReparseFile && !overwriteReparse)
            {
                fdisp.desiredOutcome = Outcome.PreserveTarget; return;
            }
            else if (destIsReparseFile)
            {
                fdisp.desiredOutcome = isNormalDir ? Outcome.ReplaceTargetWithDirectory : Outcome.ReplaceTarget;
            }
            else if (destIsNormalFile && !overwriteFile)
            {
                fdisp.desiredOutcome = Outcome.PreserveTarget; return;
            }
            else
            {
                fdisp.desiredOutcome = isNormalDir ? Outcome.ReplaceTargetWithDirectory : Outcome.ReplaceTarget;
            }
        }
        // unfortunately, we can only tell if a file is hardlinked by opening it
        // so we defer doing any tests for hardlinks until we know we need to open it anyway

    }
    public void CloneDelete(FileDisposition fdisp)
    {
        String filename = fdisp.pathName;
        String ftype = "????";
        if (fdisp.toFileInfo == null)
        {
            ftype = "NONE";
        }
        else if (fdisp.toFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint))
        {
            ftype = "SLNK";
        }
        else if (fdisp.toFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory))
        {
            ftype = "DIR ";
        }
        else
        {
            ftype = "FILE";
        }
        if (fdisp.toFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory))
        {
            if (!W32File.RemoveDirectory(addUnicodePrefix(newPath + filename)))
            {
                fdisp.actualOutcome = Outcome.Failed;
                fdisp.exception = new Win32Exception();
            }
            else
            {
                fdisp.actualOutcome = fdisp.desiredOutcome;
            };
        }
        else
        {
            if (!W32File.DeleteFile(addUnicodePrefix(newPath + filename)))
            {
                fdisp.actualOutcome = Outcome.Failed;
                fdisp.exception = new Win32Exception();
            }
            else
            {
                fdisp.actualOutcome = fdisp.desiredOutcome;

            }
        }
        deletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newPath + filename + bkFileSuffix)); // whenever we delete a target file we must delete the bkfd if it exists
        // and this call takes care of the edge condition that the bkfd is somehow
        // a directory not a normal file
        if (fdisp.exception != null)
        {
            Console.WriteLine("DELETE:{0}:{1}:{2}:{3}", ftype, prettyPrintOutcome(fdisp.actualOutcome), filename, fdisp.exception.Message);
        }
        else
        {
            Console.WriteLine("DELETE:{0}:{1}:{2}", ftype, prettyPrintOutcome(fdisp.actualOutcome), filename);
        }
    }
    public void CloneMkdir(FileDisposition fdisp)
    {

        String filename = fdisp.pathName;
        String ftype = "FILE";
        if (fdisp.fromFileInfo == null)
        {
            ftype = "NONE";
        }
        else if (fdisp.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint))
        {
            ftype = "SLNK";
        }
        else if (fdisp.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory))
        {
            ftype = "DIR ";
        }
        if (!W32File.CreateDirectory(addUnicodePrefix(newPath + filename), IntPtr.Zero))
        {
            fdisp.actualOutcome = Outcome.Failed;
            fdisp.exception = new Win32Exception();
        }
        else
        {
            fdisp.actualOutcome = fdisp.desiredOutcome;
        }
        if (fdisp.exception != null)
        {
            Console.WriteLine("MKDIR :{0}:{1}:{2}:{3}", ftype, prettyPrintOutcome(fdisp.actualOutcome), filename, fdisp.exception.Message);
        }
        else
        {
            Console.WriteLine("MKDIR :{0}:{1}:{2}", ftype, prettyPrintOutcome(fdisp.actualOutcome), filename);
        }
    }
    public void openSourceFiles(String originalFilename, out IntPtr fromFd, out IntPtr fromBkFd)
    {
        fromFd = W32File.INVALID_HANDLE_VALUE;
        fromBkFd = W32File.INVALID_HANDLE_VALUE;

        fromFd = W32File.CreateFile(addUnicodePrefix(originalPath + originalFilename)
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
            fromBkFd = W32File.CreateFile(addUnicodePrefix(originalPath + originalFilename) + bkFileSuffix
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

        W32File.DeleteFile(addUnicodePrefix(newPath + newFilename)); // we delete the file first in case it's hardlinked somewhere else
        toFd = W32File.CreateFile(addUnicodePrefix(newPath + newFilename)
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
            toBkFd = W32File.CreateFile(addUnicodePrefix(newPath + newFilename) + bkFileSuffix
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
                        setFileAttribsResult = W32File.SetFileAttributes(addUnicodePrefix(newPath + fdisp.pathName), attribsToSet);

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
        String ftype = "FILE";
        if (fdisp.fromFileInfo == null)
        {
            ftype = "NONE";
        }
        else if (fdisp.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.ReparsePoint))
        {
            ftype = "SLNK";
        }
        else if (fdisp.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory))
        {
            ftype = "DIR ";
        }
        Boolean transferNeeded = false;
        Boolean bkfTransferNeeded = false;
        Boolean doneAHardLink = false;





        if (!doneAHardLink)
        {
            switch (fdisp.desiredOutcome)
            {
                case Outcome.DeleteTarget: // do nothing here because we already have done all we need to;
                    nDeleted++;
                    break;
                case Outcome.Unchanged:
                case Outcome.PreserveTarget:
                case Outcome.IgnoredSource: // do nothing but possibly report this fact
                    nIgnored++;
                    Console.Write("NOACTN:{0}:{1}:{2}", ftype, prettyPrintOutcome(fdisp.desiredOutcome), filename);
                    break;
                case Outcome.NewDirectoryTarget:
                case Outcome.ReplaceTargetWithDirectory:// We've already created the directory, but we still need to finish the job
                    Console.Write("DIRFIX:{0}:{1}:{2} ", ftype, prettyPrintOutcome(fdisp.desiredOutcome), filename);
                    transferNeeded = true;
                    bkfTransferNeeded = createBkf;
                    nDirs++;
                    break;
                case Outcome.NewHardlinkTarget: // we don't know this yet
                case Outcome.NewTarget:
                case Outcome.ReplaceTargetDirectory:
                case Outcome.ReplaceTargetDirectoryWithHardLink: // We've already deleted the original if necessary so we just copy the new contents
                    Console.Write("COPY  :{0}:{1}:{2} ", ftype, prettyPrintOutcome(fdisp.desiredOutcome), filename);
                    transferNeeded = true;
                    bkfTransferNeeded = createBkf;
                    nFiles++;
                    break;
                case Outcome.OnlyNeedBkFile: // The main file was already OK, but we are missing a bkfd file
                    Console.Write("BKFILE:{0}:{1}:{2} ", ftype, prettyPrintOutcome(fdisp.desiredOutcome), filename);
                    transferNeeded = false;
                    bkfTransferNeeded = createBkf;
                    break;
                case Outcome.ReplaceTarget:
                case Outcome.ReplaceTargetWithHardlink: // Here we have deferred deletion of a different destination because we may be able to do an efficient rsync like transfer
                    Console.Write("OWRITE:{0}:{1}:{2} ", ftype, prettyPrintOutcome(fdisp.desiredOutcome), filename);
                    // for the moment though, we don't optimise the transfer so we delete the original file anyway unless it's a reparse directory we just created
                    if (!fdisp.fromFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory))
                    {
                        deletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newPath + filename));
                    }

                    deletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newPath + filename + bkFileSuffix));
                    transferNeeded = true;
                    bkfTransferNeeded = createBkf;
                    nOverwritten++;
                    break;
                default:
                    Console.Write("LOGIC?:{0}:{1}:{2} ", ftype, prettyPrintOutcome(fdisp.desiredOutcome), filename);
                    break;
            }
            if (transferNeeded || bkfTransferNeeded)
            {
                Boolean didHardLinkInstead = false;
                try
                {
                    if (restoreHardLinks)
                    {
                        String hardLinkName;

                        StringBuilder s = new StringBuilder(2048);
                        uint len = 2048;
                        IntPtr ffptr = FileFind.FindFirstFileNameW(addUnicodePrefix(originalPath + filename), 0, ref len, s);
                        if (ffptr != W32File.INVALID_HANDLE_VALUE)
                        {
                            hardLinkName = s.ToString();
                            if (hardlinkInfo.ContainsKey(hardLinkName))
                            {
                                // This file is a hard link to a file already processed
                                String alreadyLinkedFile = (String)hardlinkInfo[hardLinkName];
                                Boolean linkOK = false;
                                Boolean bkLinkOK = true;
                                linkOK = W32File.CreateHardLink(addUnicodePrefix(newPath + filename), addUnicodePrefix(alreadyLinkedFile), IntPtr.Zero);
                                if (createBkf) bkLinkOK = W32File.CreateHardLink(addUnicodePrefix(newPath + filename) + bkFileSuffix, alreadyLinkedFile + bkFileSuffix, IntPtr.Zero);
                                didHardLinkInstead = true;

                                if (fdisp.toFileInfo == null)
                                {
                                    fdisp.actualOutcome = Outcome.NewHardlinkTarget;
                                }
                                else
                                {
                                    fdisp.actualOutcome = Outcome.ReplaceTargetWithHardlink;
                                    if (fdisp.toFileInfo.w32fileinfo.FileAttributes.HasFlag(FileAttributes.Directory))
                                    {
                                        fdisp.actualOutcome = Outcome.ReplaceTargetDirectoryWithHardLink;
                                    }
                                }
                            }
                            else
                            {
                                hardlinkInfo[hardLinkName] = addUnicodePrefix(newPath + filename);
                                do
                                {
                                    //  Console.WriteLine("Link name:{0} so can link {1} to {2}", s, hardLinkName, hardlinkInfo[hardLinkName]);
                                    len = 2048;
                                    if (!FileFind.FindNextFileNameW(ffptr, ref len, s))
                                    {
                                        break;
                                    }
                                    hardLinkName = s.ToString();
                                    hardlinkInfo[hardLinkName] = addUnicodePrefix(newPath + filename);
                                    FileFind.FindClose(ffptr);
                                } while (true);
                            }
                        }
                    }
                    if (!didHardLinkInstead)
                    {
                        openSourceFiles(filename, out fromFd, out fromBkFd);
                        // now at last we've been forced to actually open the files, so we can see if they are hardlinked, and if so, we can try and be clever
                        if (transferNeeded) openDestFile(filename, out toFd);
                        if (bkfTransferNeeded) openDestBkFile(filename, out toBkFd);
                        TransferData(fdisp, fromFd, fromBkFd, toFd, toBkFd);
                    }
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
        if (fdisp.exception != null)
        {
            Console.WriteLine("{0}", fdisp.exception.Message);
        }
        else
        {
            Console.WriteLine();
        }

        switch (fdisp.actualOutcome) // do some stats
        {
            
        }
    }
    public bool CloneOne(String fn1, Dictionary<String, FileInfo> fileList)
    {
        String originalFilename = originalPath + fn1;
        String newFilename = newPath + fn1;

        Boolean shouldRecurse = false;
        Boolean isNormalDir = false;
        Boolean isReparseDir = false;
        Boolean isReparseFile = false;
        Boolean isNormalFile = false;

        Boolean destIsNormalDir = false;
        Boolean destIsReparseFile = false;
        Boolean destIsReparseDir = false;
        Boolean destIsNormalFile = false;
        Boolean destExisted = false;

        IntPtr toFd = W32File.INVALID_HANDLE_VALUE;
        IntPtr fromFd = W32File.INVALID_HANDLE_VALUE;
        IntPtr toBkFd = W32File.INVALID_HANDLE_VALUE;
        IntPtr fromBkFd = W32File.INVALID_HANDLE_VALUE;

        Boolean failed = true;

        byte[] md5buffer = new byte[1024 * 1034];
        long flen = 0;
        long sofar = 0;
        IntPtr wcontext = IntPtr.Zero;
        ReportStage stageReached = ReportStage.Starting;
        Outcome result = Outcome.NotFinished;

        W32File.BY_HANDLE_FILE_INFORMATION fromFileInformation = new W32File.BY_HANDLE_FILE_INFORMATION();
        W32File.BY_HANDLE_FILE_INFORMATION toFileInformation = new W32File.BY_HANDLE_FILE_INFORMATION();
        W32File.BY_HANDLE_FILE_INFORMATION toBkFdFileInformation = new W32File.BY_HANDLE_FILE_INFORMATION();
        W32File.BY_HANDLE_FILE_INFORMATION toFileInformationFromBkFd = new W32File.BY_HANDLE_FILE_INFORMATION();
        Boolean BackupFileInformationMatchesFromFile = false;
        BackupReader b = null;

        Boolean needToWriteFile = restoreFileContents;
        Boolean needToWriteBackupFile = createBkf;
        // Even if we exclude a file, we never delete it from the destination
        filesToKeep.Add(newFilename, originalFilename);
        if (createBkf)
        {
            filesToKeep.Add(newFilename + bkFileSuffix, originalFilename);
        }
        try
        {
            if (exclude(originalFilename))
            {
                nExcluded++;
               // reporter(originalFilename, newFilename, false, false, 0, 0, ReportStage.OpenOriginal, Outcome.Unchanged, null);
                result = Outcome.IgnoredSource;
                failed = false;
                return false;
            }
            stageReached = ReportStage.OpenOriginal;
            /* first - open the file itself and see what it is */

            fromFd = W32File.CreateFile(addUnicodePrefix(originalFilename)
            , W32File.EFileAccess.GenericRead
            , W32File.EFileShare.Read
            , IntPtr.Zero
            , W32File.ECreationDisposition.OpenExisting
            , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
            , IntPtr.Zero);
            if (fromFd == W32File.INVALID_HANDLE_VALUE)
            {
                result = Outcome.Failed;
                failed = true;
                return false;
            }
            /* next - see if we have a .bkfd file to go with this file */
            if (cloneAlsoUsesBkf) // otherwise we would just ignore any .bkfd files 
            {
                fromBkFd = W32File.CreateFile(addUnicodePrefix(originalFilename) + bkFileSuffix
                , W32File.EFileAccess.GenericRead
                , W32File.EFileShare.Read
                , IntPtr.Zero
                , W32File.ECreationDisposition.OpenExisting
                , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
                , IntPtr.Zero);
                // this can fail - that just means there's no existing bkfd file, which is fine
            }
            // So the source location exists and we can open it. Good start......
            W32File.GetFileInformationByHandle(fromFd, out fromFileInformation);
            // Now, was it a directory or reparse
            Boolean dorrd = fromFileInformation.FileAttributes.HasFlag(FileAttributes.Directory);
            Boolean rp = fromFileInformation.FileAttributes.HasFlag(FileAttributes.ReparsePoint);
            isNormalDir = dorrd && !rp;
            isReparseDir = dorrd && rp;
            isReparseFile = !dorrd && rp;
            isNormalFile = !dorrd && !rp;
            shouldRecurse = (isNormalDir);
            flen = (((long)fromFileInformation.FileSizeHigh) << 32) + fromFileInformation.FileSizeLow;
            //reporter(originalFilename, newFilename, isNormalDir, isReparseFile | isReparseDir, flen, sofar, ReportStage.OpenOriginal, Outcome.Unchanged, null);
            // Now see if we're interested in handling this kind of source file
            if (
                (isReparseDir && !cloneReparse) ||
                (isReparseFile && !cloneReparse) ||
                (isNormalDir && !cloneDir) ||
                (isNormalFile && !cloneFile)
                )
            {
                result = Outcome.IgnoredSource;
                failed = false; nIgnored++; return false;
            }

            stageReached = ReportStage.OpenNew;

            // At this point we know we should be dealing with the file. 
            // Now see if the destination exists and if so what it is

            toFd = W32File.CreateFile(addUnicodePrefix(newFilename)
            , W32File.EFileAccess.GenericRead
            , W32File.EFileShare.Read | W32File.EFileShare.Delete
            , IntPtr.Zero
            , W32File.ECreationDisposition.OpenExisting
            , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
            , IntPtr.Zero);

            if (toFd != W32File.INVALID_HANDLE_VALUE)
            {
                destExisted = true;
                W32File.GetFileInformationByHandle(toFd, out toFileInformation);

                if (createBkf)
                {
                    stageReached = ReportStage.OpenBkf;
                    toBkFd = W32File.CreateFile(addUnicodePrefix(newFilename) + bkFileSuffix
                      , W32File.EFileAccess.GenericRead
                      , W32File.EFileShare.Read
                      , IntPtr.Zero
                      , W32File.ECreationDisposition.OpenExisting
                      , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
                      , IntPtr.Zero);
                    if (toBkFd != W32File.INVALID_HANDLE_VALUE)
                    {
                        // do a sanity check here. Is this file a normal file of the same age as our destination. If so, it stays or goes
                        // with the destination. If it's either not a normal file, or a different age, we zap it, because it's not right
                        W32File.GetFileInformationByHandle(toBkFd, out toBkFdFileInformation);

                        if (toBkFdFileInformation.FileAttributes.HasFlag(FileAttributes.Directory) ||

                            toBkFdFileInformation.FileAttributes.HasFlag(FileAttributes.ReparsePoint)
                            )
                        {
                            W32File.CloseHandle(toBkFd);
                            toBkFd = W32File.INVALID_HANDLE_VALUE;
                            // useless so delete it
                            if (deletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename + bkFileSuffix)) <= 0)
                            {
                                result = Outcome.FailedAndBroke;

                                failed = true;
                                return false;

                            }
                        }
                        else // this bkfd exists and is usable if the outFile is OK
                        {
                            needToWriteBackupFile = false;
                            uint bytesRead;
                            uint desiredReadSize = 1024; // more than we really need
                            W32File.WIN32_STREAM_ID streamHeader;

                            IntPtr pLocalBuffer = Marshal.AllocHGlobal((int)desiredReadSize);
                            IntPtr pLocalBufferAt = pLocalBuffer;
                            toFileInformationFromBkFd.FileIndexHigh = 0;
                            toFileInformationFromBkFd.FileIndexLow = 0;
                            if (W32File.ReadFile(toBkFd, pLocalBuffer, desiredReadSize, out bytesRead, IntPtr.Zero))
                            {
                                streamHeader = (W32File.WIN32_STREAM_ID)Marshal.PtrToStructure(pLocalBufferAt, typeof(W32File.WIN32_STREAM_ID));
                                pLocalBufferAt = new IntPtr(pLocalBuffer.ToInt64() + W32File.MIN_WIN32_STREAM_ID_SIZE + CHECKSUM_SIZE); // first CHECKSUM_SIZE bytes are for a checksum
                                if (streamHeader.StreamId == (uint)W32File.StreamIdValue.BACKUP_GREENWHEEL_HEADER && streamHeader.Size > 0)
                                {

                                    toFileInformationFromBkFd = (W32File.BY_HANDLE_FILE_INFORMATION)Marshal.PtrToStructure(pLocalBufferAt, typeof(W32File.BY_HANDLE_FILE_INFORMATION));
                                }
                            }
                            W32File.CloseHandle(toBkFd);
                            toBkFd = W32File.INVALID_HANDLE_VALUE;
                            Marshal.FreeHGlobal(pLocalBuffer);
                            if (toFileInformationFromBkFd.FileSizeHigh == fromFileInformation.FileSizeHigh &&
                                toFileInformationFromBkFd.FileSizeLow == fromFileInformation.FileSizeLow &&
                                toFileInformationFromBkFd.LastWriteTime.Equals(fromFileInformation.LastWriteTime)
                                )
                            {
                                // The backup file reckons we're identical, that's good enough for me!
                                BackupFileInformationMatchesFromFile = true;
                            }

                        }
                    }
                }

                stageReached = ReportStage.OpenNew;

                Boolean dDir = toFileInformation.FileAttributes.HasFlag(FileAttributes.Directory);
                Boolean dReparse = toFileInformation.FileAttributes.HasFlag(FileAttributes.ReparsePoint);
                destIsNormalFile = !dDir && !dReparse;
                destIsNormalDir = dDir && !dReparse;
                destIsReparseDir = dDir && dReparse;
                destIsReparseFile = !dDir && dReparse;


                if (destIsNormalDir)
                {
                    if (isNormalDir)
                    {
                        // source and dest both directories so no need to worry
                    }
                    else if (isReparseFile || isReparseDir || isNormalFile)
                    {
                        if (overwriteDir)
                        {
                            int nd = deletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename));
                            if (nd > 0)
                            {
                                nDeleted += nd;
                                nOverwritten++;
                            }
                            else
                            {
                                nFailed++;
                                result = Outcome.FailedAndBroke;
                                failed = true;
                                return false;
                            }

                        }
                        else
                        {
                            result = Outcome.PreserveTarget;
                            failed = false;
                            nIgnored++;
                            return false; // since target is no use to us, don't recurse further
                        }
                    }
                }
                else if (destIsReparseFile || destIsReparseDir)
                {
                    if (isReparseDir || isReparseFile)
                    {
                        if (overwriteSameType)
                        {
                            nOverwritten++; // copy reparse point easier just to overwrite
                        }
                        else
                        {
                            result = Outcome.PreserveTarget;
                            failed = false;
                            nIgnored++;
                            return false; // since target is no use to us, don't recurse further
                        }
                    }
                    else if (isNormalDir || isNormalFile)
                    {
                        if (overwriteReparse)
                        {
                            int nd = deletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename));
                            if (nd > 0)
                            {
                                nOverwritten++;
                            }
                            else
                            {
                                nFailed++;
                                result = Outcome.Failed;
                                failed = true;
                                return false;
                            }
                        }
                        else
                        {
                            result = Outcome.PreserveTarget;
                            failed = false; nIgnored++;
                            return false; // since target is no use to us, don't recurse further
                        }
                    }

                }
                else if (destIsNormalFile)
                {
                    if (isNormalFile)
                    {
                        if (overwriteSameType)
                        {
                            // we are copying a file over, but it's possible the source and dest are the same in which case nothing needs doing

                            if (BackupFileInformationMatchesFromFile ||
                                (toFileInformation.FileSizeHigh == fromFileInformation.FileSizeHigh && toFileInformation.FileSizeLow == fromFileInformation.FileSizeLow && toFileInformation.LastWriteTime.Equals(fromFileInformation.LastWriteTime)))
                            {
                                needToWriteFile = false;
                                nSame++;
                                result = Outcome.OnlyNeedBkFile;
                                if (!needToWriteBackupFile)
                                {
                                    // not only is the original file intact, but the bkfd if wanted is also intact
                                    result = Outcome.Unchanged;
                                    failed = false;
                                    return false; // if we don't have anything to write, we can just stop.
                                }
                            }
                            else
                            {
                                int nd = deletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename));
                                if (nd > 0)
                                {
                                    nOverwritten++;
                                }
                                else
                                {
                                    nFailed++;
                                    result = Outcome.Failed;
                                    failed = true;
                                    return false;
                                }
                            }
                        }
                        else
                        {
                            result = Outcome.PreserveTarget;
                            failed = false; nIgnored++;
                            return false;
                        }
                    }
                    else if (isNormalDir || isReparseFile || isReparseDir)
                    {
                        if (overwriteFile)
                        {
                            int nd = deletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename));
                            if (nd > 0)
                            {
                                result = Outcome.ReplaceTarget;
                                nOverwritten++;
                            }
                            else
                            {
                                nFailed++;
                                result = Outcome.FailedAndBroke;
                                failed = true;
                                return false;
                            }
                        }
                        else
                        {
                            result = Outcome.PreserveTarget;
                            failed = false; nIgnored++;
                            return false; // since target is no use to us, don't recurse further
                        }
                    }
                }
            } // End toFd was a valid handle.
            W32File.CloseHandle(toFd);
            toFd = W32File.INVALID_HANDLE_VALUE;
            // At this point we are still going, which means our destination no longer exists if it did before
            // What happens next depends on whether we are hardlinking or copying
            stageReached = ReportStage.CreateTarget;
            if (needToWriteFile)
            {
                if (isNormalDir || isReparseDir)
                {
                    result = destExisted ? Outcome.ReplaceTarget : Outcome.NewTarget;
                    if (isNormalDir && destExisted && destIsNormalDir)
                    {
                        if (needToWriteBackupFile) result = Outcome.OnlyNeedBkFile;
                        else result = Outcome.Unchanged;
                    }
                    if (!W32File.CreateDirectory(addUnicodePrefix(newFilename), IntPtr.Zero))
                    {

                    }
                    toFd = W32File.CreateFile(addUnicodePrefix(newFilename)
                   , W32File.EFileAccess.GenericWrite | W32File.EFileAccess.WriteOwner | W32File.EFileAccess.WriteDAC
                   , W32File.EFileShare.Read | W32File.EFileShare.Write | W32File.EFileShare.Delete
                   , IntPtr.Zero
                   , W32File.ECreationDisposition.OpenExisting // cos we've just created it above
                   , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
                   , IntPtr.Zero);
                }
                else // normal file or file reparse
                {
                    if (useHardLinks) // a special case not well integrated with bkf etc
                    {
                        Boolean linkOK = W32File.CreateHardLink(addUnicodePrefix(newFilename), addUnicodePrefix(originalFilename), IntPtr.Zero);
                        if (!linkOK)
                        {
                            result = Outcome.Failed;
                            failed = true;
                            nFailed++;
                            return false;
                        }
                        result = Outcome.NewHardlinkTarget;
                        failed = false;
                        return false;
                    }
                    //
                    if (isNormalFile && restoreHardLinks && (fromFileInformation.NumberOfLinks > 1))
                    {
                        // Detected hard linked source file
                        String linkId = fromFileInformation.FileIndexHigh.ToString() + "," + fromFileInformation.FileIndexLow.ToString();
                        // Console.WriteLine("{0} has {1} hard links and id {2}", originalFilename, fromFileInformation.NumberOfLinks,linkId);
                        if (hardlinkInfo.ContainsKey(linkId))
                        {
                            String alreadyLinkedFile = (String)hardlinkInfo[linkId];
                            Boolean linkOK = false;
                            Boolean bkLinkOK = true;
                            linkOK = W32File.CreateHardLink(addUnicodePrefix(newFilename), addUnicodePrefix(alreadyLinkedFile), IntPtr.Zero);
                            if (createBkf) bkLinkOK = W32File.CreateHardLink(addUnicodePrefix(newFilename) + bkFileSuffix, alreadyLinkedFile + bkFileSuffix, IntPtr.Zero);
                            if (linkOK && bkLinkOK)
                            {
                                result = destExisted ? Outcome.ReplaceTargetWithHardlink : Outcome.NewHardlinkTarget;
                                nInternalHardLinked++;
                                failed = false;
                                return false;
                            }
                            else
                            {
                                result = Outcome.Failed;
                                failed = true;
                                return false;
                            }
                        }
                        else
                        {
                            // this is the first encounter with this link so create the target as normal
                            hardlinkInfo[linkId] = newFilename;
                        }
                        /* pretty but slow alternative
                        StringBuilder s = new StringBuilder(2048);
                        uint len = 2048;
                        IntPtr ffptr = FileFind.FindFirstFileNameW(originalFilename, 0, ref len, s);
                        String hardLinkName = s.ToString();
                        if (hardlinkInfo.ContainsKey(hardLinkName))
                        {
                            // we already have processed this hard link - so we can just create a hard link and we're done
                            String alreadyLinkedFile = (String)hardlinkInfo[hardLinkName];
                            Boolean linkOK = false;
                            Boolean bkLinkOK = true;
                            linkOK = W32File.CreateHardLink(addUnicodePrefix(newFilename), addUnicodePrefix(alreadyLinkedFile), IntPtr.Zero);
                            if (createBkf) bkLinkOK = W32File.CreateHardLink(addUnicodePrefix(newFilename) + bkFileSuffix, alreadyLinkedFile + bkFileSuffix, IntPtr.Zero);
                            reporter(originalFilename, newFilename, isNormalDir, isReparseFile || isReparseDir, flen, sofar, ReportStage.HardLink, (linkOK && bkLinkOK) ? null : new Win32Exception());
                            FileFind.FindClose(ffptr);
                            failed = false;
                            return false;
                        }
                        hardlinkInfo[hardLinkName] = newFilename;
                        do
                        {
                            Console.WriteLine("Link name:{0}", s);
                            len = 2048;
                            if (!FileFind.FindNextFileNameW(ffptr, ref len, s))
                            {
                                break;
                            }
                            hardLinkName = s.ToString();
                            hardlinkInfo[hardLinkName] = newFilename;
                        } while (true);
                        FileFind.FindClose(ffptr);
                      */
                    }

                    // Normal files get created here
                    if (restoreFileContents || isReparseFile)
                    {
                        W32File.DeleteFile(addUnicodePrefix(newFilename)); // we delete the file first in case it's hardlinked somewhere else
                        toFd = W32File.CreateFile(addUnicodePrefix(newFilename)
                               , W32File.EFileAccess.GenericWrite | W32File.EFileAccess.WriteOwner | W32File.EFileAccess.WriteDAC
                               , W32File.EFileShare.Read | W32File.EFileShare.Write | W32File.EFileShare.Delete
                               , IntPtr.Zero
                               , W32File.ECreationDisposition.CreateAlways
                               , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
                               , IntPtr.Zero);
                        result = result = destExisted ? Outcome.ReplaceTarget : Outcome.NewTarget;
                    }
                } // end if normal file
            }
            // At this stage we have open handles for the up to 4 files we need to use
            if (needToWriteFile && (toFd == W32File.INVALID_HANDLE_VALUE))
            {
                result = destExisted ? Outcome.FailedAndBroke : Outcome.Failed;
                failed = true;
                return false;
            }

            // If we are working with backup file format, we also create the corresponding backup file, and in this case we just clobber if necessary
            if (createBkf && toBkFd == W32File.INVALID_HANDLE_VALUE)
            {
                toBkFd = W32File.CreateFile(addUnicodePrefix(newFilename) + bkFileSuffix
            , W32File.EFileAccess.GenericWrite | W32File.EFileAccess.WriteOwner | W32File.EFileAccess.WriteDAC
            , W32File.EFileShare.Read | W32File.EFileShare.Write | W32File.EFileShare.Delete
            , IntPtr.Zero
            , W32File.ECreationDisposition.OpenAlways
            , 0
            , IntPtr.Zero);
                if (toBkFd == W32File.INVALID_HANDLE_VALUE)
                {
                    stageReached = ReportStage.OpenBkf;
                    result = destExisted ? Outcome.FailedAndBroke : Outcome.Failed;

                    failed = true;
                    return false;
                }
                needToWriteBackupFile = true;
            }
            else
            {
                toBkFd = W32File.INVALID_HANDLE_VALUE;
                needToWriteBackupFile = false;
            }


            // Now our source and target are all lined up, so is there anything to do?
            if (!needToWriteFile && !needToWriteBackupFile)
            {
                result = Outcome.Unchanged;
                failed = false; return shouldRecurse; //Nothing to do here but clean up in the finally block
            }

            uint bytesWritten = 0;
            // System.Threading.NativeOverlapped nov = new System.Threading.NativeOverlapped(); // we don't use this any more as it caused trouble
            stageReached = ReportStage.Transfer;

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
                        if (b.atEndOfFile) { failed = false; break; } // we were done
                        // something has gone wrong?
                        continue;
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
                            failed = true; break;
                        }
                        if (b.streamDataSegmentSize != bytesWritten)
                        {
                            failed = true; break;
                        }

                    }
                    if (currentStreamGoesToChecksum && !b.atNewStreamHeader)
                    {
                        Marshal.Copy(b.streamDataSegmentStart, md5buffer, 0, (int)b.streamDataSegmentSize);
                        md5.TransformBlock(md5buffer, 0, (int)b.streamDataSegmentSize, null, 0);
                    }
                    sofar += b.streamDataSegmentSize;
                    //reporter(originalFilename, newFilename, isNormalDir, isReparseFile || isReparseDir, flen, sofar, ReportStage.Transfer, Outcome.NotFinished, null);
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
            failed = false;
            return shouldRecurse;
        }
        finally
        {
            uint dummy;
            Win32Exception w = null;
            if (failed) w = new Win32Exception(); // capture what failed so far
            else
            {
                stageReached = ReportStage.CleanUp;
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
                        failed = true;

                        result = Outcome.FailedAndBroke;
                    };
                }
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
                        failed = true;
                        result = Outcome.FailedAndBroke;
                    };
                }
                if (failed) w = new Win32Exception();
            }

            Boolean setFileAttribsResult;
            if (!failed)
            {
                if (needToWriteFile)
                {
                    W32File.SetFileTime(toFd, ref fromFileInformation.CreationTime, ref fromFileInformation.LastAccessTime, ref fromFileInformation.LastWriteTime);
                    if (restoreAttributes)
                    {
                        FileAttributes mask = FileAttributes.ReadOnly | FileAttributes.Hidden | FileAttributes.System | FileAttributes.Normal | FileAttributes.Archive;
                        FileAttributes attribsToSet = fromFileInformation.FileAttributes & mask;
                        setFileAttribsResult = W32File.SetFileAttributes(newFilename, attribsToSet);

                    }
                };
                if (needToWriteBackupFile)
                {
                    W32File.SetFileTime(toBkFd, ref fromFileInformation.CreationTime, ref fromFileInformation.LastAccessTime, ref fromFileInformation.LastWriteTime);
                    // this allows us to tell if the bkfd is in sync with its companion file
                };
            }
            W32File.CloseHandle(fromFd);
            W32File.CloseHandle(toFd);

            W32File.CloseHandle(fromBkFd);
            W32File.CloseHandle(toBkFd);

            if (!failed)
            {
                if (isReparseFile || isReparseDir) nSpecial++;
                else if (isNormalDir) nDirs++;
                else nFiles++;
                stageReached = ReportStage.Finished;
            }
            else
            {
                nFailed++;
            }

            //reporter(originalFilename, newFilename, isNormalDir, isReparseDir || isReparseFile, flen, sofar, stageReached, result, w);
        }
        // return shouldRecurse;
    }
}

/*
public class RobocopyHelper
{
    // this is an earlier failed attempt to cheat by using robocopy to do clearups
    // but robocopy doesn't handle \\?\ filenames passed as parameters properly, so it's not general enough and we don't do it this way any more
    public static Boolean DeleteDirectoryRecursively(string directoryPath)
    {
        var tempEmptySourceDir = new DirectoryInfo(Path.GetTempPath() + "\\TempEmptyDir-" + Guid.NewGuid());
        try
        {
            tempEmptySourceDir.Create();
            using (  var process = new Process())
            {
                process.StartInfo.FileName = "robocopy.exe";
                process.StartInfo.Arguments = "\"" + tempEmptySourceDir.FullName + "\"  \"" + directoryPath + "\" /purge /r:1 /w:1 /np /xj /sl";
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.CreateNoWindow = true;
                process.Start();
                process.WaitForExit();
            }
            tempEmptySourceDir.Delete();
            new DirectoryInfo(directoryPath).Attributes = FileAttributes.Normal;
            Directory.Delete(directoryPath);
        }
        catch(IOException) {
            return false;
        }
        return true;
    }
}
*/