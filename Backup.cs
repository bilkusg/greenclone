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
class DeletionHelper
{
    // if you want a job done properly.....
    // The methods in this class will delete a directory and all its contents by hook or crook
    private static Hashtable emptyHashtable = new Hashtable();

    public static int DeletePathAndContentsRegardless(string foundFileName)
    {
        return DeletePathAndContentsUnless(foundFileName, emptyHashtable);
    }
    public static int DeletePathAndContentsUnless(string foundFileName, Hashtable filesToKeep)
    {
        // this no nonsense method will do its damndest to delete a path regardless of what it is or what ( if a directory ) it contains.
        // if you have backup rights and the path is legitimate, it should always work.....if not there's yet another edge case to worry about.
        int nDeleted = 0;
        FileAttributes attribs = W32File.GetFileAttributes(foundFileName);

        if (attribs.HasFlag(FileAttributes.Directory) && !attribs.HasFlag(FileAttributes.ReparsePoint))
        {
            int nd =
             DeleteDirectoryContentsRecursivelyUnless(foundFileName, filesToKeep);
            if (nd < 0) return nd;
            nDeleted += nd;
        }
        if (filesToKeep.ContainsKey(foundFileName)) // this file is one of ours, so hands off!
        {
            return nDeleted;
        }
        if (attribs.HasFlag(FileAttributes.ReadOnly))
        {
            W32File.SetFileAttributes(foundFileName, FileAttributes.Normal);
        }
        if (attribs.HasFlag(FileAttributes.Directory))
        {
            if (!W32File.RemoveDirectory(foundFileName))
            {
                return -1;
            };
            nDeleted++;
        }
        else
        {
            if (!W32File.DeleteFile(foundFileName))
            {
                return -1;
            };
            nDeleted++;
        }

        return nDeleted;
    }
    public static int DeleteDirectoryContentsRecursively(string directoryPath)
    {
        return DeleteDirectoryContentsRecursivelyUnless(directoryPath, emptyHashtable);
    }
    public static int DeleteDirectoryContentsRecursivelyUnless(string directoryPath, Hashtable filesToKeep)
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
    protected virtual Boolean exclude(String fromFilename)
    {
        // the default excluder - can be overriden in derived classes
        // return true if this file should not be copied based on its name only
        if (excludeList == null) return false;
        if (fromFilename.Length <= originalPathLength) return false;
        String pathToMatch = @"\" + fromFilename.Substring(originalPathLength) + @"\";
        foreach (String exclusion in excludeList)
        {
            if (pathToMatch.Contains(exclusion)) return true;
        }
        return false;
    }
    public Backup(String of, String nf)
    {
        originalPath = of;
        newPath = nf;
        char[] backslashes = { '\\' };
        // Because we support long filenames, we use windows functions which don't understand forward slashes. But because we do support forward slashes, we convert them here
        originalPath = originalPath.Replace(@"/", @"\");
        newPath = newPath.Replace(@"/", @"\");
        // These should be directories without trailing slashes. Since we're nice, we'll remove trailing slashes
        if (originalPath.EndsWith(@"\"))
        {
            originalPath = originalPath.TrimEnd(backslashes);
        }
        if (newPath.EndsWith(@"\"))
        {
            newPath = newPath.TrimEnd(backslashes);
        }

        originalPathLength = of.Length;


        // reporter = defaultReportFunc;
    }
    public enum reportStage
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

    public enum outcome
    {
        NotFinished = -1,
        IgnoredSource = 0,
        NewTarget = 1,
        NewHardlinkTarget = 11,

        ReplaceTarget = 31,
        ReplaceTargetWithHardlink = 32,

        PreserveTarget = 41,

        Unchanged = 51,
        // bad outcomes, failed means failed and did nothing, failedAndBroke means did something but not the right thing!
        Failed = 99,
        FailedAndBroke = 100

    }
    public static int reportVerbosity = 1;

    // override this in a derived class if you want to report differently
    protected virtual void reporter(String originalFilename, String newFilename, Boolean isDir, Boolean isSpecial, long filelen, long donesofar, reportStage stage, outcome finalOutcome, Win32Exception e)
    {
        if (reportVerbosity == 0) return;
        String wasWhat = "";
        if (isDir) wasWhat = "DIR ";
        else if (isSpecial) wasWhat = "SLNK";
        else wasWhat = "FILE";
        String didWhat;
        String path = originalFilename.Substring(originalPathLength - 1);
        if (reportVerbosity < 2)
        {
            Console.Write("{0}:{1,12:N0}:", wasWhat, filelen);
        }

        if (finalOutcome == outcome.Failed)
        {
            if (e == null) e = new Win32Exception();
            Console.WriteLine("FAIL:{0}:{1}", path, e.Message);
            return;
        }
        if (finalOutcome == outcome.FailedAndBroke)
        {
            if (e == null) e = new Win32Exception();
            Console.WriteLine("BRKN:{0}:{1}", path, e.Message);
            return;
        }


        switch (finalOutcome)
        {
            case outcome.PreserveTarget:
                didWhat = "PRSV"; break;
            case outcome.IgnoredSource:
                didWhat = "SKIP"; break;
            case outcome.NewHardlinkTarget:
                didWhat = "NHLK"; break;
            case outcome.Unchanged:
                didWhat = "SAME"; break;
            case outcome.NewTarget:
                didWhat = "NEW "; break;
            case outcome.NotFinished:
                didWhat = "ABRT"; break;
            case outcome.ReplaceTarget:
                didWhat = "OVWR"; break;
            case outcome.ReplaceTargetWithHardlink:
                didWhat = "RHLK"; break;
            default:
                didWhat = "????";
                break;
        }

        switch (stage)
        {
            case reportStage.OpenOriginal:
                if (reportVerbosity > 2)
                {
                    Console.Write("{0}:{1,12:N0}:", wasWhat, filelen);
                }
                break;
            case reportStage.Transfer:
                if (reportVerbosity > 5)
                {
                    Console.WriteLine();
                    Console.Write("..{0}:{1}..", donesofar, filelen);
                }
                break;
            default:
                if (reportVerbosity > 2)
                    Console.WriteLine("{0}:{1}", didWhat, path);
                break;
        }
    }

    // public delegate void reportFunc(String originalFilename, String newFilename, Boolean isDir, Boolean isSpecial, long filelen, long donesofar, reportStage stage, outcome finalOutcome, Win32Exception e);
    // public reportFunc reporter;

    public delegate Boolean cloneFunc(String originalFilenane, String newFilename);

    virtual public void doit()
    {
        if (recursive)
        {
            CloneRecursive(originalPath, newPath, CloneOne);
        }
        else
        {
            CloneOne(originalPath, newPath);
        }
        if (removeExtra)
        {
            nDeleted = DeletionHelper.DeleteDirectoryContentsRecursivelyUnless(newPath, filesToKeep);
        }
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
    public void CloneRecursive(string source, string dest, cloneFunc cloner)
    {
        /* If this is a .bkfd file, then don't clone it! */
        if (source.EndsWith(bkFileSuffix))
        {
            return;
        }
        Boolean shouldRecurse = cloner(source, dest); // This may throw an exception if the reporter wants to exit if there's an error
        if (!shouldRecurse) return;
        FileFind.WIN32_FIND_DATA fdata = new FileFind.WIN32_FIND_DATA();
        IntPtr findhandle;
        findhandle = FileFind.FindFirstFile(source + @"\*", ref fdata);
        if (findhandle != W32File.INVALID_HANDLE_VALUE)
        {
            do
            {
                if ((fdata.cFileName != @".") && (fdata.cFileName != @".."))
                {
                    CloneRecursive(source + @"\" + fdata.cFileName, dest + @"\" + fdata.cFileName, cloner);
                }
            }
            while (FileFind.FindNextFile(findhandle, ref fdata));
            FileFind.FindClose(findhandle);
        };
    }

    // This is the low level function which does all the work by default
    public bool CloneOne(String originalFilename, String newFilename)
    {
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


        IntPtr pBuffer = Marshal.AllocHGlobal(102400);
        long flen = 0;
        long sofar = 0;
        IntPtr wcontext = IntPtr.Zero;
        reportStage stageReached = reportStage.Starting;
        outcome result = outcome.NotFinished;

        W32File.BY_HANDLE_FILE_INFORMATION fromFileInformation = new W32File.BY_HANDLE_FILE_INFORMATION();
        W32File.BY_HANDLE_FILE_INFORMATION toFileInformation = new W32File.BY_HANDLE_FILE_INFORMATION();
        W32File.BY_HANDLE_FILE_INFORMATION toBkFdFileInformation = new W32File.BY_HANDLE_FILE_INFORMATION();

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
                reporter(originalFilename, newFilename, false, false, 0, 0, reportStage.OpenOriginal, outcome.Unchanged, null);
                result = outcome.IgnoredSource;
                failed = false;
                return false;
            }
            stageReached = reportStage.OpenOriginal;
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
                result = outcome.Failed;
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
            reporter(originalFilename, newFilename, isNormalDir, isReparseFile | isReparseDir, flen, sofar, reportStage.OpenOriginal, outcome.Unchanged, null);
            // Now see if we're interested in handling this kind of source file
            if (
                (isReparseDir && !cloneReparse) ||
                (isReparseFile && !cloneReparse) ||
                (isNormalDir && !cloneDir) ||
                (isNormalFile && !cloneFile)
                )
            {
                result = outcome.IgnoredSource;
                failed = false; nIgnored++; return false;
            }

            stageReached = reportStage.OpenNew;

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
                    stageReached = reportStage.OpenBkf;
                    toBkFd = W32File.CreateFile(addUnicodePrefix(newFilename + bkFileSuffix)
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
                        W32File.CloseHandle(toBkFd);
                        toBkFd = W32File.INVALID_HANDLE_VALUE;

                        if (toBkFdFileInformation.FileAttributes.HasFlag(FileAttributes.Directory) ||

                            toBkFdFileInformation.FileAttributes.HasFlag(FileAttributes.ReparsePoint) ||
                           !toFileInformation.LastWriteTime.Equals(toBkFdFileInformation.LastWriteTime)

                            )
                        {
                            // useless so delete it
                            if (DeletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename + bkFileSuffix)) <= 0)
                            {
                                result = outcome.FailedAndBroke;
                                failed = true;
                                return false;

                            }
                        }
                        else // this bkfd exists and is usable if the outFile is OK
                        {
                            needToWriteBackupFile = false;
                        }
                    }
                }

                stageReached = reportStage.OpenNew;

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
                            int nd = DeletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename));
                            if (nd > 0)
                            {
                                nDeleted += nd;
                                nOverwritten++;
                            }
                            else
                            {
                                nFailed++;
                                result = outcome.FailedAndBroke;
                                failed = true;
                                return false;
                            }

                        }
                        else
                        {
                            result = outcome.PreserveTarget;
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
                            result = outcome.PreserveTarget;
                            failed = false;
                            nIgnored++;
                            return false; // since target is no use to us, don't recurse further
                        }
                    }
                    else if (isNormalDir || isNormalFile)
                    {
                        if (overwriteReparse)
                        {
                            int nd = DeletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename));
                            if (nd > 0)
                            {
                                nOverwritten++;
                            }
                            else
                            {
                                nFailed++;
                                result = outcome.Failed;
                                failed = true;
                                return false;
                            }
                        }
                        else
                        {
                            result = outcome.PreserveTarget;
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

                            if (toFileInformation.FileSizeHigh == fromFileInformation.FileSizeHigh && toFileInformation.FileSizeLow == fromFileInformation.FileSizeLow && toFileInformation.LastWriteTime.Equals(fromFileInformation.LastWriteTime))
                            {
                                needToWriteFile = false;
                                nSame++;
                                result = outcome.Unchanged;
                                if (!needToWriteBackupFile)
                                {
                                    // not only is the original file intact, but the bkfd if wanted is also intact
                                    result = outcome.Unchanged;
                                    failed = false;
                                    return false; // if we don't have anything to write, we can just stop.
                                }
                            }
                            else
                            {
                                int nd = DeletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename));
                                if (nd > 0)
                                {
                                    nOverwritten++;
                                }
                                else
                                {
                                    nFailed++;
                                    result = outcome.Failed;
                                    failed = true;
                                    return false;
                                }
                            }
                        }
                        else
                        {
                            result = outcome.PreserveTarget;
                            failed = false; nIgnored++;
                            return false;
                        }
                    }
                    else if (isNormalDir || isReparseFile || isReparseDir)
                    {
                        if (overwriteFile)
                        {
                            int nd = DeletionHelper.DeletePathAndContentsRegardless(addUnicodePrefix(newFilename));
                            if (nd > 0)
                            {
                                result = outcome.ReplaceTarget;
                                nOverwritten++;
                            }
                            else
                            {
                                nFailed++;
                                result = outcome.FailedAndBroke;
                                failed = true;
                                return false;
                            }
                        }
                        else
                        {
                            result = outcome.PreserveTarget;
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
            stageReached = reportStage.CreateTarget;
            if (needToWriteFile)
            {
                if (isNormalDir || isReparseDir)
                {
                    result = destExisted ? outcome.ReplaceTarget : outcome.NewTarget;
                    if (isNormalDir && destExisted && destIsNormalDir) result = outcome.Unchanged;
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
                            result = outcome.Failed;
                            failed = true;
                            nFailed++;
                            return false;
                        }
                        result = outcome.NewHardlinkTarget;
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
                                result = destExisted ? outcome.ReplaceTargetWithHardlink : outcome.NewHardlinkTarget;
                                nInternalHardLinked++;
                                failed = false;
                                return false;
                            }
                            else
                            {
                                result = outcome.Failed;
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
                            reporter(originalFilename, newFilename, isNormalDir, isReparseFile || isReparseDir, flen, sofar, reportStage.HardLink, (linkOK && bkLinkOK) ? null : new Win32Exception());
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
                        toFd = W32File.CreateFile(addUnicodePrefix(newFilename)
                               , W32File.EFileAccess.GenericWrite | W32File.EFileAccess.WriteOwner | W32File.EFileAccess.WriteDAC
                               , W32File.EFileShare.Read | W32File.EFileShare.Write | W32File.EFileShare.Delete
                               , IntPtr.Zero
                               , W32File.ECreationDisposition.OpenAlways
                               , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
                               , IntPtr.Zero);
                        result = result = destExisted ? outcome.ReplaceTarget : outcome.NewTarget;
                    }
                } // end if normal file
            }
            // At this stage we have open handles for the up to 4 files we need to use
            if (toFd == W32File.INVALID_HANDLE_VALUE)
            {
                result = destExisted ? outcome.FailedAndBroke : outcome.Failed;
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
                    stageReached = reportStage.OpenBkf;
                    result = destExisted ? outcome.FailedAndBroke : outcome.Failed;

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
                result = outcome.Unchanged;
                failed = false; return false; //Nothing to do here but clean up in the finally block
            }

            uint bytesWritten = 0;
            // System.Threading.NativeOverlapped nov = new System.Threading.NativeOverlapped(); // we don't use this any more as it caused trouble
            stageReached = reportStage.Transfer;

            b = new BackupReader(fromFd, fromBkFd);
            Boolean currentStreamGoesToOutBkFd = false;
            Boolean currentStreamGoesToOutFd = false;
            Boolean alreadySeenSecurityData = false;
            Boolean alreadyWrittenGreenwheelHeader = false;
            do
            {
                // if we have a fromBkFd and a fromFd we merge the two to create a stream
                // if we only have one, we use it directly
                // all this is encapsulated in b
                // The end result should be that we get a sequence of stream headers and stream data in exactly the right format for a BackupWrite to restore
                // the file we want. We can then pick and choose individual streams to go to the outBkFd if we are producing one.
                // It is an open question whether backup write cares about the precise order of the streams but we play safe. 
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
                            greenStream.Size = 0;
                            greenStream.StreamNameSize = 0;
                            greenStream.StreamNameData = IntPtr.Zero;
                            greenStream.StreamAttributes = (uint)fromFileInformation.FileAttributes;

                            uint nbw = 0;
                            int greenStreamSize = Marshal.SizeOf(greenStream);
                            IntPtr greenStreamPtr = Marshal.AllocHGlobal(greenStreamSize);
                            Marshal.StructureToPtr(greenStream, greenStreamPtr, false);

                            bool ok = W32File.WriteFile(toBkFd, greenStreamPtr, W32File.MIN_WIN32_STREAM_ID_SIZE, out nbw, IntPtr.Zero);
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
                                    break;
                                }
                            case (uint)W32File.StreamIdValue.BACKUP_EA_DATA:
                                {
                                    currentStreamGoesToOutFd = needToWriteFile && restoreAFS;
                                    currentStreamGoesToOutBkFd = needToWriteBackupFile && createBkfAFS;
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
                sofar += b.streamDataSegmentSize;
                reporter(originalFilename, newFilename, isNormalDir, isReparseFile || isReparseDir, flen, sofar, reportStage.Transfer, outcome.NotFinished, null);
            }
            while (true);
            failed = false;
            return shouldRecurse;
        }
        finally
        {
            uint dummy;
            Win32Exception w = null;
            stageReached = reportStage.CleanUp;
            if (failed) w = new Win32Exception(); // capture what failed so far
            else
            {
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

                        result = outcome.FailedAndBroke;
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
                        result = outcome.FailedAndBroke;
                    };
                }
                if (failed) w = new Win32Exception();
            }

            Marshal.FreeHGlobal(pBuffer);

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
            }
            else
            {
                nFailed++;
            }
            stageReached = reportStage.Finished;
            reporter(originalFilename, newFilename, isNormalDir, isReparseDir || isReparseFile, flen, sofar, stageReached, result, w);
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