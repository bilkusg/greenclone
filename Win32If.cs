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
// Acknowledgements to pinvoke.net for some of the signatures of methods in this file

using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;
using System.IO;
using System.ComponentModel;
namespace Win32IF
{
    public class W32File
    {
        public static readonly IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);
        [Flags]
        public enum EMethod : uint
        {
            Buffered = 0,
            InDirect = 1,
            OutDirect = 2,
            Neither = 3
        }
        [Flags]
        public enum EFileDevice : uint
        {
            Beep = 0x00000001,
            CDRom = 0x00000002,
            CDRomFileSytem = 0x00000003,
            Controller = 0x00000004,
            Datalink = 0x00000005,
            Dfs = 0x00000006,
            Disk = 0x00000007,
            DiskFileSystem = 0x00000008,
            FileSystem = 0x00000009,
            InPortPort = 0x0000000a,
            Keyboard = 0x0000000b,
            Mailslot = 0x0000000c,
            MidiIn = 0x0000000d,
            MidiOut = 0x0000000e,
            Mouse = 0x0000000f,
            MultiUncProvider = 0x00000010,
            NamedPipe = 0x00000011,
            Network = 0x00000012,
            NetworkBrowser = 0x00000013,
            NetworkFileSystem = 0x00000014,
            Null = 0x00000015,
            ParallelPort = 0x00000016,
            PhysicalNetcard = 0x00000017,
            Printer = 0x00000018,
            Scanner = 0x00000019,
            SerialMousePort = 0x0000001a,
            SerialPort = 0x0000001b,
            Screen = 0x0000001c,
            Sound = 0x0000001d,
            Streams = 0x0000001e,
            Tape = 0x0000001f,
            TapeFileSystem = 0x00000020,
            Transport = 0x00000021,
            Unknown = 0x00000022,
            Video = 0x00000023,
            VirtualDisk = 0x00000024,
            WaveIn = 0x00000025,
            WaveOut = 0x00000026,
            Port8042 = 0x00000027,
            NetworkRedirector = 0x00000028,
            Battery = 0x00000029,
            BusExtender = 0x0000002a,
            Modem = 0x0000002b,
            Vdm = 0x0000002c,
            MassStorage = 0x0000002d,
            Smb = 0x0000002e,
            Ks = 0x0000002f,
            Changer = 0x00000030,
            Smartcard = 0x00000031,
            Acpi = 0x00000032,
            Dvd = 0x00000033,
            FullscreenVideo = 0x00000034,
            DfsFileSystem = 0x00000035,
            DfsVolume = 0x00000036,
            Serenum = 0x00000037,
            Termsrv = 0x00000038,
            Ksec = 0x00000039,
            // From Windows Driver Kit 7
            Fips = 0x0000003A,
            Infiniband = 0x0000003B,
            Vmbus = 0x0000003E,
            CryptProvider = 0x0000003F,
            Wpd = 0x00000040,
            Bluetooth = 0x00000041,
            MtComposite = 0x00000042,
            MtTransport = 0x00000043,
            Biometric = 0x00000044,
            Pmi = 0x00000045
        }
        [Flags]
        public enum EIOControlCode : uint
        {
            // STORAGE
            StorageBase = EFileDevice.MassStorage,
            StorageCheckVerify = (StorageBase << 16) | (0x0200 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            StorageCheckVerify2 = (StorageBase << 16) | (0x0200 << 2) | EMethod.Buffered | (0 << 14), // FileAccess.Any
            StorageMediaRemoval = (StorageBase << 16) | (0x0201 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            StorageEjectMedia = (StorageBase << 16) | (0x0202 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            StorageLoadMedia = (StorageBase << 16) | (0x0203 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            StorageLoadMedia2 = (StorageBase << 16) | (0x0203 << 2) | EMethod.Buffered | (0 << 14),
            StorageReserve = (StorageBase << 16) | (0x0204 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            StorageRelease = (StorageBase << 16) | (0x0205 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            StorageFindNewDevices = (StorageBase << 16) | (0x0206 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            StorageEjectionControl = (StorageBase << 16) | (0x0250 << 2) | EMethod.Buffered | (0 << 14),
            StorageMcnControl = (StorageBase << 16) | (0x0251 << 2) | EMethod.Buffered | (0 << 14),
            StorageGetMediaTypes = (StorageBase << 16) | (0x0300 << 2) | EMethod.Buffered | (0 << 14),
            StorageGetMediaTypesEx = (StorageBase << 16) | (0x0301 << 2) | EMethod.Buffered | (0 << 14),
            StorageResetBus = (StorageBase << 16) | (0x0400 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            StorageResetDevice = (StorageBase << 16) | (0x0401 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            StorageGetDeviceNumber = (StorageBase << 16) | (0x0420 << 2) | EMethod.Buffered | (0 << 14),
            StoragePredictFailure = (StorageBase << 16) | (0x0440 << 2) | EMethod.Buffered | (0 << 14),
            StorageObsoleteResetBus = (StorageBase << 16) | (0x0400 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            StorageObsoleteResetDevice = (StorageBase << 16) | (0x0401 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            // DISK
            DiskBase = EFileDevice.Disk,
            DiskGetDriveGeometry = (DiskBase << 16) | (0x0000 << 2) | EMethod.Buffered | (0 << 14),
            DiskGetPartitionInfo = (DiskBase << 16) | (0x0001 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskSetPartitionInfo = (DiskBase << 16) | (0x0002 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskGetDriveLayout = (DiskBase << 16) | (0x0003 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskSetDriveLayout = (DiskBase << 16) | (0x0004 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskVerify = (DiskBase << 16) | (0x0005 << 2) | EMethod.Buffered | (0 << 14),
            DiskFormatTracks = (DiskBase << 16) | (0x0006 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskReassignBlocks = (DiskBase << 16) | (0x0007 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskPerformance = (DiskBase << 16) | (0x0008 << 2) | EMethod.Buffered | (0 << 14),
            DiskIsWritable = (DiskBase << 16) | (0x0009 << 2) | EMethod.Buffered | (0 << 14),
            DiskLogging = (DiskBase << 16) | (0x000a << 2) | EMethod.Buffered | (0 << 14),
            DiskFormatTracksEx = (DiskBase << 16) | (0x000b << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskHistogramStructure = (DiskBase << 16) | (0x000c << 2) | EMethod.Buffered | (0 << 14),
            DiskHistogramData = (DiskBase << 16) | (0x000d << 2) | EMethod.Buffered | (0 << 14),
            DiskHistogramReset = (DiskBase << 16) | (0x000e << 2) | EMethod.Buffered | (0 << 14),
            DiskRequestStructure = (DiskBase << 16) | (0x000f << 2) | EMethod.Buffered | (0 << 14),
            DiskRequestData = (DiskBase << 16) | (0x0010 << 2) | EMethod.Buffered | (0 << 14),
            DiskControllerNumber = (DiskBase << 16) | (0x0011 << 2) | EMethod.Buffered | (0 << 14),
            DiskSmartGetVersion = (DiskBase << 16) | (0x0020 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskSmartSendDriveCommand = (DiskBase << 16) | (0x0021 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskSmartRcvDriveData = (DiskBase << 16) | (0x0022 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskUpdateDriveSize = (DiskBase << 16) | (0x0032 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskGrowPartition = (DiskBase << 16) | (0x0034 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskGetCacheInformation = (DiskBase << 16) | (0x0035 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskSetCacheInformation = (DiskBase << 16) | (0x0036 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskDeleteDriveLayout = (DiskBase << 16) | (0x0040 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskFormatDrive = (DiskBase << 16) | (0x00f3 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            DiskSenseDevice = (DiskBase << 16) | (0x00f8 << 2) | EMethod.Buffered | (0 << 14),
            DiskCheckVerify = (DiskBase << 16) | (0x0200 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskMediaRemoval = (DiskBase << 16) | (0x0201 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskEjectMedia = (DiskBase << 16) | (0x0202 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskLoadMedia = (DiskBase << 16) | (0x0203 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskReserve = (DiskBase << 16) | (0x0204 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskRelease = (DiskBase << 16) | (0x0205 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskFindNewDevices = (DiskBase << 16) | (0x0206 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            DiskGetMediaTypes = (DiskBase << 16) | (0x0300 << 2) | EMethod.Buffered | (0 << 14),
            // CHANGER
            ChangerBase = EFileDevice.Changer,
            ChangerGetParameters = (ChangerBase << 16) | (0x0000 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            ChangerGetStatus = (ChangerBase << 16) | (0x0001 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            ChangerGetProductData = (ChangerBase << 16) | (0x0002 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            ChangerSetAccess = (ChangerBase << 16) | (0x0004 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            ChangerGetElementStatus = (ChangerBase << 16) | (0x0005 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            ChangerInitializeElementStatus = (ChangerBase << 16) | (0x0006 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            ChangerSetPosition = (ChangerBase << 16) | (0x0007 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            ChangerExchangeMedium = (ChangerBase << 16) | (0x0008 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            ChangerMoveMedium = (ChangerBase << 16) | (0x0009 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            ChangerReinitializeTarget = (ChangerBase << 16) | (0x000A << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            ChangerQueryVolumeTags = (ChangerBase << 16) | (0x000B << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            // FILESYSTEM
            FsctlRequestOplockLevel1 = (EFileDevice.FileSystem << 16) | (0 << 2) | EMethod.Buffered | (0 << 14),
            FsctlRequestOplockLevel2 = (EFileDevice.FileSystem << 16) | (1 << 2) | EMethod.Buffered | (0 << 14),
            FsctlRequestBatchOplock = (EFileDevice.FileSystem << 16) | (2 << 2) | EMethod.Buffered | (0 << 14),
            FsctlOplockBreakAcknowledge = (EFileDevice.FileSystem << 16) | (3 << 2) | EMethod.Buffered | (0 << 14),
            FsctlOpBatchAckClosePending = (EFileDevice.FileSystem << 16) | (4 << 2) | EMethod.Buffered | (0 << 14),
            FsctlOplockBreakNotify = (EFileDevice.FileSystem << 16) | (5 << 2) | EMethod.Buffered | (0 << 14),
            FsctlLockVolume = (EFileDevice.FileSystem << 16) | (6 << 2) | EMethod.Buffered | (0 << 14),
            FsctlUnlockVolume = (EFileDevice.FileSystem << 16) | (7 << 2) | EMethod.Buffered | (0 << 14),
            FsctlDismountVolume = (EFileDevice.FileSystem << 16) | (8 << 2) | EMethod.Buffered | (0 << 14),
            FsctlIsVolumeMounted = (EFileDevice.FileSystem << 16) | (10 << 2) | EMethod.Buffered | (0 << 14),
            FsctlIsPathnameValid = (EFileDevice.FileSystem << 16) | (11 << 2) | EMethod.Buffered | (0 << 14),
            FsctlMarkVolumeDirty = (EFileDevice.FileSystem << 16) | (12 << 2) | EMethod.Buffered | (0 << 14),
            FsctlQueryRetrievalPointers = (EFileDevice.FileSystem << 16) | (14 << 2) | EMethod.Neither | (0 << 14),
            FsctlGetCompression = (EFileDevice.FileSystem << 16) | (15 << 2) | EMethod.Buffered | (0 << 14),
            FsctlSetCompression = (EFileDevice.FileSystem << 16) | (16 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            FsctlMarkAsSystemHive = (EFileDevice.FileSystem << 16) | (19 << 2) | EMethod.Neither | (0 << 14),
            FsctlOplockBreakAckNo2 = (EFileDevice.FileSystem << 16) | (20 << 2) | EMethod.Buffered | (0 << 14),
            FsctlInvalidateVolumes = (EFileDevice.FileSystem << 16) | (21 << 2) | EMethod.Buffered | (0 << 14),
            FsctlQueryFatBpb = (EFileDevice.FileSystem << 16) | (22 << 2) | EMethod.Buffered | (0 << 14),
            FsctlRequestFilterOplock = (EFileDevice.FileSystem << 16) | (23 << 2) | EMethod.Buffered | (0 << 14),
            FsctlFileSystemGetStatistics = (EFileDevice.FileSystem << 16) | (24 << 2) | EMethod.Buffered | (0 << 14),
            FsctlGetNtfsVolumeData = (EFileDevice.FileSystem << 16) | (25 << 2) | EMethod.Buffered | (0 << 14),
            FsctlGetNtfsFileRecord = (EFileDevice.FileSystem << 16) | (26 << 2) | EMethod.Buffered | (0 << 14),
            FsctlGetVolumeBitmap = (EFileDevice.FileSystem << 16) | (27 << 2) | EMethod.Neither | (0 << 14),
            FsctlGetRetrievalPointers = (EFileDevice.FileSystem << 16) | (28 << 2) | EMethod.Neither | (0 << 14),
            FsctlMoveFile = (EFileDevice.FileSystem << 16) | (29 << 2) | EMethod.Buffered | (0 << 14),
            FsctlIsVolumeDirty = (EFileDevice.FileSystem << 16) | (30 << 2) | EMethod.Buffered | (0 << 14),
            FsctlGetHfsInformation = (EFileDevice.FileSystem << 16) | (31 << 2) | EMethod.Buffered | (0 << 14),
            FsctlAllowExtendedDasdIo = (EFileDevice.FileSystem << 16) | (32 << 2) | EMethod.Neither | (0 << 14),
            FsctlReadPropertyData = (EFileDevice.FileSystem << 16) | (33 << 2) | EMethod.Neither | (0 << 14),
            FsctlWritePropertyData = (EFileDevice.FileSystem << 16) | (34 << 2) | EMethod.Neither | (0 << 14),
            FsctlFindFilesBySid = (EFileDevice.FileSystem << 16) | (35 << 2) | EMethod.Neither | (0 << 14),
            FsctlDumpPropertyData = (EFileDevice.FileSystem << 16) | (37 << 2) | EMethod.Neither | (0 << 14),
            FsctlSetObjectId = (EFileDevice.FileSystem << 16) | (38 << 2) | EMethod.Buffered | (0 << 14),
            FsctlGetObjectId = (EFileDevice.FileSystem << 16) | (39 << 2) | EMethod.Buffered | (0 << 14),
            FsctlDeleteObjectId = (EFileDevice.FileSystem << 16) | (40 << 2) | EMethod.Buffered | (0 << 14),
            FsctlSetReparsePoint = (EFileDevice.FileSystem << 16) | (41 << 2) | EMethod.Buffered | (0 << 14),
            FsctlGetReparsePoint = (EFileDevice.FileSystem << 16) | (42 << 2) | EMethod.Buffered | (0 << 14),
            FsctlDeleteReparsePoint = (EFileDevice.FileSystem << 16) | (43 << 2) | EMethod.Buffered | (0 << 14),
            FsctlEnumUsnData = (EFileDevice.FileSystem << 16) | (44 << 2) | EMethod.Neither | (0 << 14),
            FsctlSecurityIdCheck = (EFileDevice.FileSystem << 16) | (45 << 2) | EMethod.Neither | (FileAccess.Read << 14),
            FsctlReadUsnJournal = (EFileDevice.FileSystem << 16) | (46 << 2) | EMethod.Neither | (0 << 14),
            FsctlSetObjectIdExtended = (EFileDevice.FileSystem << 16) | (47 << 2) | EMethod.Buffered | (0 << 14),
            FsctlCreateOrGetObjectId = (EFileDevice.FileSystem << 16) | (48 << 2) | EMethod.Buffered | (0 << 14),
            FsctlSetSparse = (EFileDevice.FileSystem << 16) | (49 << 2) | EMethod.Buffered | (0 << 14),
            FsctlSetZeroData = (EFileDevice.FileSystem << 16) | (50 << 2) | EMethod.Buffered | (FileAccess.Write << 14),
            FsctlQueryAllocatedRanges = (EFileDevice.FileSystem << 16) | (51 << 2) | EMethod.Neither | (FileAccess.Read << 14),
            FsctlEnableUpgrade = (EFileDevice.FileSystem << 16) | (52 << 2) | EMethod.Buffered | (FileAccess.Write << 14),
            FsctlSetEncryption = (EFileDevice.FileSystem << 16) | (53 << 2) | EMethod.Neither | (0 << 14),
            FsctlEncryptionFsctlIo = (EFileDevice.FileSystem << 16) | (54 << 2) | EMethod.Neither | (0 << 14),
            FsctlWriteRawEncrypted = (EFileDevice.FileSystem << 16) | (55 << 2) | EMethod.Neither | (0 << 14),
            FsctlReadRawEncrypted = (EFileDevice.FileSystem << 16) | (56 << 2) | EMethod.Neither | (0 << 14),
            FsctlCreateUsnJournal = (EFileDevice.FileSystem << 16) | (57 << 2) | EMethod.Neither | (0 << 14),
            FsctlReadFileUsnData = (EFileDevice.FileSystem << 16) | (58 << 2) | EMethod.Neither | (0 << 14),
            FsctlWriteUsnCloseRecord = (EFileDevice.FileSystem << 16) | (59 << 2) | EMethod.Neither | (0 << 14),
            FsctlExtendVolume = (EFileDevice.FileSystem << 16) | (60 << 2) | EMethod.Buffered | (0 << 14),
            FsctlQueryUsnJournal = (EFileDevice.FileSystem << 16) | (61 << 2) | EMethod.Buffered | (0 << 14),
            FsctlDeleteUsnJournal = (EFileDevice.FileSystem << 16) | (62 << 2) | EMethod.Buffered | (0 << 14),
            FsctlMarkHandle = (EFileDevice.FileSystem << 16) | (63 << 2) | EMethod.Buffered | (0 << 14),
            FsctlSisCopyFile = (EFileDevice.FileSystem << 16) | (64 << 2) | EMethod.Buffered | (0 << 14),
            FsctlSisLinkFiles = (EFileDevice.FileSystem << 16) | (65 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            FsctlHsmMsg = (EFileDevice.FileSystem << 16) | (66 << 2) | EMethod.Buffered | ((FileAccess.Read | FileAccess.Write) << 14),
            FsctlNssControl = (EFileDevice.FileSystem << 16) | (67 << 2) | EMethod.Buffered | (FileAccess.Write << 14),
            FsctlHsmData = (EFileDevice.FileSystem << 16) | (68 << 2) | EMethod.Neither | ((FileAccess.Read | FileAccess.Write) << 14),
            FsctlRecallFile = (EFileDevice.FileSystem << 16) | (69 << 2) | EMethod.Neither | (0 << 14),
            FsctlNssRcontrol = (EFileDevice.FileSystem << 16) | (70 << 2) | EMethod.Buffered | (FileAccess.Read << 14),
            // VIDEO
            VideoQuerySupportedBrightness = (EFileDevice.Video << 16) | (0x0125 << 2) | EMethod.Buffered | (0 << 14),
            VideoQueryDisplayBrightness = (EFileDevice.Video << 16) | (0x0126 << 2) | EMethod.Buffered | (0 << 14),
            VideoSetDisplayBrightness = (EFileDevice.Video << 16) | (0x0127 << 2) | EMethod.Buffered | (0 << 14)
        }


        [Flags]
        public enum EFileAccess : uint
        {
            Delete = 0x10000,
            ReadControl = 0x20000,
            WriteDAC = 0x40000,
            WriteOwner = 0x80000,
            Synchronize = 0x100000,

            StandardRightsRequired = 0xF0000,
            StandardRightsRead = ReadControl,
            StandardRightsWrite = ReadControl,
            StandardRightsExecute = ReadControl,
            StandardRightsAll = 0x1F0000,
            SpecificRightsAll = 0xFFFF,

            AccessSystemSecurity = 0x1000000,       // AccessSystemAcl access type

            MaximumAllowed = 0x2000000,         // MaximumAllowed access type

            GenericRead = 0x80000000,
            GenericWrite = 0x40000000,
            GenericExecute = 0x20000000,
            GenericAll = 0x10000000
        }
        [Flags]
        public enum EFileShare : uint
        {
            /// <summary>
            /// 
            /// </summary>
            None = 0x00000000,
            /// <summary>
            /// Enables subsequent open operations on an object to request read access. 
            /// Otherwise, other processes cannot open the object if they request read access. 
            /// If this flag is not specified, but the object has been opened for read access, the function fails.
            /// </summary>
            Read = 0x00000001,
            /// <summary>
            /// Enables subsequent open operations on an object to request write access. 
            /// Otherwise, other processes cannot open the object if they request write access. 
            /// If this flag is not specified, but the object has been opened for write access, the function fails.
            /// </summary>
            Write = 0x00000002,
            /// <summary>
            /// Enables subsequent open operations on an object to request delete access. 
            /// Otherwise, other processes cannot open the object if they request delete access.
            /// If this flag is not specified, but the object has been opened for delete access, the function fails.
            /// </summary>
            Delete = 0x00000004
        }
        public enum ECreationDisposition : uint
        {
            /// <summary>
            /// Creates a new file. The function fails if a specified file exists.
            /// </summary>
            New = 1,
            /// <summary>
            /// Creates a new file, always. 
            /// If a file exists, the function overwrites the file, clears the existing attributes, combines the specified file attributes, 
            /// and flags with FILE_ATTRIBUTE_ARCHIVE, but does not set the security descriptor that the SECURITY_ATTRIBUTES structure specifies.
            /// </summary>
            CreateAlways = 2,
            /// <summary>
            /// Opens a file. The function fails if the file does not exist. 
            /// </summary>
            OpenExisting = 3,
            /// <summary>
            /// Opens a file, always. 
            /// If a file does not exist, the function creates a file as if dwCreationDisposition is CREATE_NEW.
            /// </summary>
            OpenAlways = 4,
            /// <summary>
            /// Opens a file and truncates it so that its size is 0 (zero) bytes. The function fails if the file does not exist.
            /// The calling process must open the file with the GENERIC_WRITE access right. 
            /// </summary>
            TruncateExisting = 5
        }
        [Flags]
        public enum EFileAttributes : uint
        {
            Readonly = 0x00000001,
            Hidden = 0x00000002,
            System = 0x00000004,
            Directory = 0x00000010,
            Archive = 0x00000020,
            Device = 0x00000040,
            Normal = 0x00000080,
            Temporary = 0x00000100,
            SparseFile = 0x00000200,
            ReparsePoint = 0x00000400,
            Compressed = 0x00000800,
            Offline = 0x00001000,
            NotContentIndexed = 0x00002000,
            Encrypted = 0x00004000,
            Write_Through = 0x80000000,
            Overlapped = 0x40000000,
            NoBuffering = 0x20000000,
            RandomAccess = 0x10000000,
            SequentialScan = 0x08000000,
            DeleteOnClose = 0x04000000,
            BackupSemantics = 0x02000000,
            PosixSemantics = 0x01000000,
            OpenReparsePoint = 0x00200000,
            OpenNoRecall = 0x00100000,
            FirstPipeInstance = 0x00080000
        }
        [StructLayout(LayoutKind.Sequential)]
        public struct WIN32_STREAM_ID
        {
            public uint StreamId;
            public uint StreamAttributes;
            public UInt64 Size;
            public uint StreamNameSize;
            public IntPtr StreamNameData; // really a variable length array
        }
        public const int MIN_WIN32_STREAM_ID_SIZE = 20;
        public enum StreamIdValue : uint
        {
            BACKUP_ALTERNATE_DATA = 0x4, //Alternative data streams. This corresponds to the NTFS $DATA stream type on a named data stream.
            BACKUP_DATA = 0x1,// Standard data. This corresponds to the NTFS $DATA stream type on the default (unnamed) data stream.
            BACKUP_EA_DATA = 0x2, //Extended attribute data. This corresponds to the NTFS $EA stream type.
            BACKUP_LINK = 0x5, // Hard link information. This corresponds to the NTFS $FILE_NAME stream type.
            BACKUP_OBJECT_ID = 0x7, //Objects identifiers. This corresponds to the NTFS $OBJECT_ID stream type.
            BACKUP_PROPERTY_DATA = 0x6, // Property data.
            BACKUP_REPARSE_DATA = 0x8, //Reparse points. This corresponds to the NTFS $REPARSE_POINT stream type.
            BACKUP_SECURITY_DATA = 0x3, // Security descriptor data.
            BACKUP_SPARSE_BLOCK = 0x9, // Sparse file. This corresponds to the NTFS $DATA stream type for a sparse file.
            BACKUP_TXFS_DATA = 0xA,// Transactional NTFS (TxF) data stream. This corresponds to the NTFS $TXF_DATA stream type
            BACKUP_GREENWHEEL_HEADER = 0x29 // My own stream ID value to allow me to misuse the structure to store extra information in my backup files
        }

        // See pinvoke definition for CreateFile for the various Enums.
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern IntPtr CreateFile(
                                        string lpFileName,
                                        EFileAccess dwDesiredAccess,
                                        EFileShare dwShareMode,
                                        IntPtr lpSecurityAttributes,
                                        ECreationDisposition dwCreationDisposition,
                                        EFileAttributes dwFlagsAndAttributes,
                                        IntPtr hTemplateFile);


        [DllImport("kernel32.dll",SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern bool ReadFile(IntPtr hFile, IntPtr lpBuffer,
                      uint nNumberOfBytesToRead, out uint lpNumberOfBytesRead,
                      IntPtr n);
//                      ref System.Threading.NativeOverlapped lpOverlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool WriteFile(IntPtr hFile, IntPtr lpBuffer,
                      uint nNumberOfBytesToWrite, out uint lpNumberOfBytesWritten,
            //ref System.Threading.NativeOverlapped lpOverlapped);
                      IntPtr n);
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool SetFilePointerEx(
             IntPtr hFile, long liDistanceToMove,
             out long lpNewFilePointer, uint dwMoveMethod);
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool BackupRead(IntPtr handle,
                                            IntPtr buffer,
                                            uint bytesToRead,
                                            out uint bytesRead,
                                            bool abort,
                                            bool processSecurity,
                                            ref IntPtr context);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool BackupWrite(IntPtr handle,
                                            IntPtr buffer,
                                            uint bytesToWrite,
                                            out uint bytesWritten,
                                            bool abort,
                                            bool processSecurity,
                                            ref IntPtr context);
        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool CloseHandle(IntPtr hObject);

        // See the PInvoke definition of DeviceIoControl for EIOControlCode
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern bool DeviceIoControl(
                                        IntPtr hDevice,
                                        EIOControlCode dwIoControlCode,
                                        IntPtr InBuffer,
                                        int nInBufferSize,
                                        IntPtr OutBuffer,
                                        int nOutBufferSize,
                                        ref int pBytesReturned,
                                        IntPtr lpOverlapped
        );

        [StructLayout(LayoutKind.Sequential)]
        public struct BY_HANDLE_FILE_INFORMATION
        {
            public FileAttributes FileAttributes;
            public System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
            public uint VolumeSerialNumber;
            public uint FileSizeHigh;
            public uint FileSizeLow;
            public uint NumberOfLinks;
            public uint FileIndexHigh;
            public uint FileIndexLow;
        }
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool GetFileInformationByHandle(IntPtr hFile,
                                        out BY_HANDLE_FILE_INFORMATION lpFileInformation);


        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern Boolean SetFileAttributes(string FileName, FileAttributes attributes);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern FileAttributes GetFileAttributes(string FileName);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern Boolean PathFileExists(string FileName);




        [StructLayout(LayoutKind.Sequential)]
        public struct WIN32_FILE_ATTRIBUTE_DATA
        {
            public EFileAttributes dwFileAttributes;
            public System.Runtime.InteropServices.ComTypes.FILETIME ftCreationTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME ftLastAccessTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME ftLastWriteTime;
            public uint nFileSizeHigh;
            public uint nFileSizeLow;
        };
        public enum GET_FILEEX_INFO_LEVELS
        {
            GetFileExInfoStandard,
            GetFileExMaxInfoLevel
        };


        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GetFileAttributesEx(string lpFileName, GET_FILEEX_INFO_LEVELS fInfoLevelId, out WIN32_FILE_ATTRIBUTE_DATA fileData);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern bool CreateDirectory(string path, IntPtr securityAttributes);
        [DllImport("kernel32.dll", SetLastError = true)]

        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool SetFileTime(IntPtr hFile, ref System.Runtime.InteropServices.ComTypes.FILETIME lpCreationTime, ref System.Runtime.InteropServices.ComTypes.FILETIME lpLastAccessTime, ref System.Runtime.InteropServices.ComTypes.FILETIME lpLastWriteTime);
        
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern bool CreateHardLink(string FileName, string ExistingFileName, IntPtr SecurityAttributes);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern bool DeleteFile(string path);
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern bool RemoveDirectory(string lpPathName);
    }
    public class FileFind
    {
        public const int MAX_PATH = 260;
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        public struct WIN32_FIND_DATA
        {
            public int dwFileAttributes;
            public System.Runtime.InteropServices.ComTypes.FILETIME ftCreationTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME ftLastAccessTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME ftLastWriteTime;
            public uint nFileSizeHigh;
            public uint nFileSizeLow;
            public uint dwReserved0;
            public uint dwReserved1;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = MAX_PATH)]
            public string cFileName;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 14)]
            public string cAlternate;
        }
        public enum FINDEX_SEARCH_OPS
        {
            FindExSearchNameMatch = 0,
            FindExSearchLimitToDirectories = 1,
            FindExSearchLimitToDevices = 2
        }
        public enum FINDEX_INFO_LEVELS
        {
            FindExInfoStandard = 0,
            FindExInfoBasic = 1
        }
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern IntPtr FindFirstFileEx(
            string lpFileName,
            FINDEX_INFO_LEVELS fInfoLevelId,
            out WIN32_FIND_DATA lpFindFileData,
            FINDEX_SEARCH_OPS fSearchOp,
            IntPtr lpSearchFilter,
            int dwAdditionalFlags);

        [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern IntPtr FindFirstFile(string lpFileName, ref WIN32_FIND_DATA lpFindFileData);
        [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern bool FindNextFile(System.IntPtr hFindFile, ref WIN32_FIND_DATA lpFindFileData);
        [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern int FindClose(System.IntPtr hFindFile);

        [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern IntPtr FindFirstFileNameW(string lpFileName, UInt32 flags,ref uint StringLength, [Out] StringBuilder linkName);
        [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern bool FindNextFileNameW(System.IntPtr hFindFile, ref uint StringLength,[Out] StringBuilder linkName);
    }
    public class Privileges
    {
        [StructLayout(LayoutKind.Sequential)]
        public struct LUID
        {
            public int LowPart;
            public int HighPart;
        }
        [StructLayout(LayoutKind.Sequential)]
        public struct TOKEN_PRIVILEGES
        {
            public int PrivilegeCount;
            public LUID Luid;
            public int Attributes;
        }

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode)]
        public static extern bool OpenProcessToken(int ProcessHandle, int DesiredAccess,
        ref IntPtr tokenhandle);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
        public static extern int GetCurrentProcess();

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode)]
        public static extern bool LookupPrivilegeValue(string lpsystemname, string lpname,
        [MarshalAs(UnmanagedType.Struct)] ref LUID lpLuid);

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode)]
        public static extern bool AdjustTokenPrivileges(IntPtr tokenhandle, bool disableprivs,
        [MarshalAs(UnmanagedType.Struct)]ref TOKEN_PRIVILEGES Newstate, int bufferlength,
        IntPtr Previousstatefudge, IntPtr returnlengthfudge);
        //[MarshalAs(UnmanagedType.Struct)]ref TOKEN_PRIVILEGES PreviousState,ref IntPtr Returnlength);

        public const int TOKEN_ADJUST_PRIVILEGES = 0x00000020;
        public const int TOKEN_QUERY = 0x00000008;
        public const int SE_PRIVILEGE_ENABLED = 0x00000002;
        public const string SE_RESTORE_NAME = "SeRestorePrivilege";
        public const string SE_BACKUP_NAME = "SeBackupPrivilege";
        static Boolean ModifyPrivilege(string requestedPrivilege)
        {
            IntPtr processToken = IntPtr.Zero;

            if (!OpenProcessToken(GetCurrentProcess(),
                          TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY,
                          ref processToken))
            {
                return false;
            }
            LUID luid;
            luid.LowPart = 0;
            luid.HighPart = 0;

            if (!LookupPrivilegeValue(null,
                                requestedPrivilege,
                                ref luid))
            {
                return false;
            }
            TOKEN_PRIVILEGES NewState;
            NewState.PrivilegeCount = 1;
            NewState.Luid = luid;
            NewState.Attributes = SE_PRIVILEGE_ENABLED;

            // Adjust the token privilege.
            if (!AdjustTokenPrivileges(processToken,
                              false,
                              ref NewState,
                              0, IntPtr.Zero, IntPtr.Zero
                               ))
            {
                return false;
            }
            return true;
        }

        public static Boolean obtainPrivileges()
        {
            if (!ModifyPrivilege(SE_BACKUP_NAME)) return false;
            if (!ModifyPrivilege(SE_RESTORE_NAME)) return false ;
            return true;
        }

    }
    public class NTFSReparse
    {
        public enum ReparseTagType : uint
        {
            IO_REPARSE_TAG_MOUNT_POINT = (0xA0000003),
            IO_REPARSE_TAG_HSM = (0xC0000004),
            IO_REPARSE_TAG_SIS = (0x80000007),
            IO_REPARSE_TAG_DFS = (0x8000000A),
            IO_REPARSE_TAG_SYMLINK = (0xA000000C),
            IO_REPARSE_TAG_DFSR = (0x80000012),
        }
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        public struct REPARSE_DATA_BUFFER
        {
            internal uint ReparseTag;
            internal ushort ReparseDataLength;
            ushort Reserved;
            internal ushort SubstituteNameOffset;
            internal ushort SubstituteNameLength;
            internal ushort PrintNameOffset;
            internal ushort PrintNameLength;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 0x3FF0 / 2)] // 16384 bytes
            internal char[] PathBuffer;
        }

        public const int MAXIMUM_REPARSE_DATA_BUFFER_SIZE = 16500;
        public static REPARSE_DATA_BUFFER getReparseDataBuffer(String filename)
        {
            REPARSE_DATA_BUFFER rdb;
            IntPtr hMP = W32File.CreateFile(filename
            , W32File.EFileAccess.GenericRead
            , 0
            , IntPtr.Zero
            , W32File.ECreationDisposition.OpenExisting
            , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
            , IntPtr.Zero);
            if (hMP == W32File.INVALID_HANDLE_VALUE)
            {
                throw new Win32Exception();
            }
            IntPtr pMem = Marshal.AllocHGlobal(MAXIMUM_REPARSE_DATA_BUFFER_SIZE);
            int bytesReturned = 0;
            try
            {

                bool rc = W32File.DeviceIoControl(hMP

                    , W32File.EIOControlCode.FsctlGetReparsePoint
                    , IntPtr.Zero
                    , 0
                    , pMem
                    , MAXIMUM_REPARSE_DATA_BUFFER_SIZE
                    , ref bytesReturned
                    , IntPtr.Zero);
                if (!rc)
                {
                    throw new Win32Exception();
                }
                rdb = (REPARSE_DATA_BUFFER)Marshal.PtrToStructure(pMem, typeof(REPARSE_DATA_BUFFER));
                return rdb;
            }
            finally
            {
                Marshal.FreeHGlobal(pMem);
                W32File.CloseHandle(hMP);
            }
        }
        public static Boolean getReparseData(ref REPARSE_DATA_BUFFER rdb, out ReparseTagType tag, out String targetName, out String printName)
        {
            targetName = "";
            printName = "";

            tag = (ReparseTagType)rdb.ReparseTag;
            int addoffset = 0;
            if (tag == ReparseTagType.IO_REPARSE_TAG_SYMLINK) addoffset = 2;
            targetName = new String(rdb.PathBuffer, rdb.SubstituteNameOffset / 2 + addoffset, rdb.SubstituteNameLength / 2);
            printName = new String(rdb.PathBuffer, rdb.PrintNameOffset / 2 + addoffset, rdb.PrintNameLength / 2);
            return true;
        }
        public static Boolean getReparseData(String filename, out ReparseTagType tag, out String targetName, out String printName)
        {
            targetName = "";
            printName = "";
            tag = (ReparseTagType)0;

            REPARSE_DATA_BUFFER rdb;
            try
            {
                rdb = getReparseDataBuffer(filename);
            }
            catch
            {
                return false;
            }
            return getReparseData(ref rdb, out tag, out targetName, out printName);
        }
        public static REPARSE_DATA_BUFFER createReparseDataBuffer(ReparseTagType tag, String targetName, String printName)
        {
            if (!targetName.StartsWith(@"\\?\"))
            {
                targetName = @"\\?\" + targetName;
            }
            int headerSize = 0;
            REPARSE_DATA_BUFFER rdb = new REPARSE_DATA_BUFFER();
            rdb.ReparseTag = (uint)tag;
            // Setup the string information
            rdb.PathBuffer = new char[16384 / sizeof(char)];
            long startPos = 0;
            if (tag == ReparseTagType.IO_REPARSE_TAG_SYMLINK)
            {
                rdb.PathBuffer[0] = (char)0; // fudging the flags field
                rdb.PathBuffer[1] = (char)0;
                startPos = 2; //chars
                headerSize = 16; //bytes
            }
            else if (tag == ReparseTagType.IO_REPARSE_TAG_MOUNT_POINT)
            {
                startPos = 0; //chars
                headerSize = 12; //bytes
            }

            ushort targetLenBytes = (ushort)(targetName.ToCharArray().Length * sizeof(char));
            rdb.SubstituteNameLength = targetLenBytes;
            rdb.SubstituteNameOffset = 0;

            targetName.ToCharArray().CopyTo(rdb.PathBuffer, startPos);
            startPos += targetName.ToCharArray().Length;
            rdb.PathBuffer[startPos++] = (char)0;
            ushort printLenBytes = (ushort)(printName.ToCharArray().Length * sizeof(char));
            rdb.PrintNameOffset = (ushort)(targetLenBytes + sizeof(char));
            if (printLenBytes > sizeof(char))
            {
                printName.ToCharArray().CopyTo(rdb.PathBuffer, startPos);
                startPos += printName.ToCharArray().Length;
                rdb.PathBuffer[startPos++] = (char)0;
                rdb.PrintNameLength = printLenBytes;
            }
            else
            {
                rdb.PrintNameLength = 0;
                printLenBytes = 0;
            }


            rdb.ReparseDataLength = (ushort)(headerSize + targetLenBytes + printLenBytes);
            return rdb;
        }
        public static void createReparseFromBuffer(String rppFolder, ref REPARSE_DATA_BUFFER rdb, Boolean isDirectory)
        {
            const int ReparseHeaderSize = sizeof(uint) + 2 * sizeof(ushort);
            if (Directory.Exists(rppFolder))
                Directory.Delete(rppFolder, true);
            if (isDirectory) Directory.CreateDirectory(rppFolder);

            // Open the file to with correct access to write the Reparse Point
            IntPtr hMP = W32File.CreateFile(rppFolder
            , W32File.EFileAccess.GenericWrite
            , W32File.EFileShare.Read | W32File.EFileShare.Write | W32File.EFileShare.Delete
            , IntPtr.Zero
            , W32File.ECreationDisposition.OpenAlways
            , W32File.EFileAttributes.BackupSemantics | W32File.EFileAttributes.OpenReparsePoint
            , IntPtr.Zero);
            if (hMP == new IntPtr(-1))
            {
                throw new Win32Exception();
            }

            // The size of the data we are passing into the FSCTL.
            int dataSize = rdb.ReparseDataLength + ReparseHeaderSize;

            IntPtr pMem = Marshal.AllocHGlobal(Marshal.SizeOf(rdb));
            try
            {
                Marshal.StructureToPtr(rdb, pMem, false);

                // Set the reparse point.
                int bytesReturned = 0;
                bool rc = W32File.DeviceIoControl(hMP
                    , W32File.EIOControlCode.FsctlSetReparsePoint
                    , pMem
                    , dataSize
                    , IntPtr.Zero
                    , 0
                    , ref bytesReturned
                    , IntPtr.Zero);
                if (!rc)
                {
                    int err = Marshal.GetLastWin32Error();
                    throw new Win32Exception();
                }
            }
            finally
            {
                Marshal.FreeHGlobal(pMem);
                W32File.CloseHandle(hMP);
            }
        }
        public static void createReparse(String rppFolder, ReparseTagType tag, String targetName, String printName, Boolean isDirectory)
        {
            REPARSE_DATA_BUFFER rdb = createReparseDataBuffer(tag, targetName, printName);
            createReparseFromBuffer(rppFolder, ref rdb, isDirectory);
        }
    } 
}