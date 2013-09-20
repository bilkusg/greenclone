/* This module provides an interface to the AlphaVSS libraries (www.alphaleonis.com) which provide
 * a .net wrapper for the functionality of VSS on recent ( XP and later ) versions of Windows
 * It is a very cut down interface as we are not interested in most of what VSS can do, and we want to keep it simple.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Text.RegularExpressions;
using System.ComponentModel;
using System.Collections.ObjectModel;
using System.Diagnostics;
using Alphaleonis.Win32.Vss;
using System.Runtime.InteropServices;
using System.Collections;


namespace VSS
{
    public class VssHelper
    {
        // maps from drive letters to the results of the shadow once successful
        public Hashtable pathToShadow = new Hashtable();
        public Hashtable pathToId = new Hashtable();
        public Guid snapshotSetId;
        public Boolean snapshotExists = false;

        IVssBackupComponents _BackupComponents = null;
        IVssImplementation VssImplementation = null;
        
        public VssHelper()
        {
        }

        public void CreateShadowsForDrives(string[] drives,Boolean fullShadow)
        {
            if (fullShadow)
            {
                Marshal.ThrowExceptionForHR(NativeMethods.CoInitializeSecurity(IntPtr.Zero, -1, IntPtr.Zero, IntPtr.Zero, NativeMethods.RpcAuthnLevel.None, NativeMethods.RpcImpLevel.Impersonate, IntPtr.Zero, NativeMethods.EoAuthnCap.None, IntPtr.Zero));
            }
            if ( VssImplementation == null)
            {
                try
                {
                    VssImplementation = VssUtils.LoadImplementation();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("ERR :VSS INIT:{0}", ex.Message);
                    Console.WriteLine("You probably don't have the AlphaVSS.??.???.dll files in the program directory");
                    return;
                    // print a message and die
                }
            }
            _BackupComponents = VssImplementation.CreateVssBackupComponents();
            _BackupComponents.InitializeForBackup(null);
            _BackupComponents.SetBackupState(false, true, VssBackupType.Full, false);
            _BackupComponents.GatherWriterMetadata();
            snapshotSetId = _BackupComponents.StartSnapshotSet();
            snapshotExists = true;
            for (int i = 0; i < drives.Length; i++)
            {
                Guid SnapshotId = _BackupComponents.AddToSnapshotSet(drives[i], Guid.Empty);
                pathToId.Add(drives[i], SnapshotId);
            }
            _BackupComponents.PrepareForBackup();
            _BackupComponents.DoSnapshotSet();

            for (int i = 0; i < drives.Length; i++)
            {
                Guid SnapshotId = (Guid)pathToId[drives[i]];
                VssSnapshotProperties v = _BackupComponents.GetSnapshotProperties(SnapshotId);
                pathToShadow.Add(drives[i],v.SnapshotDeviceObject);
            }
       }
        public void DeleteShadows()
        {
            if (snapshotExists)
            {
                _BackupComponents.DeleteSnapshotSet(snapshotSetId, true);
            }
        }
    }

    class NativeMethods
    {
        public enum RpcAuthnLevel
        {
            Default = 0,
            None = 1,
            Connect = 2,
            Call = 3,
            Pkt = 4,
            PktIntegrity = 5,
            PktPrivacy = 6
        }

        public enum RpcImpLevel
        {
            Default = 0,
            Anonymous = 1,
            Identify = 2,
            Impersonate = 3,
            Delegate = 4
        }

        public enum EoAuthnCap
        {
            None = 0x00,
            MutualAuth = 0x01,
            StaticCloaking = 0x20,
            DynamicCloaking = 0x40,
            AnyAuthority = 0x80,
            MakeFullSIC = 0x100,
            Default = 0x800,
            SecureRefs = 0x02,
            AccessControl = 0x04,
            AppID = 0x08,
            Dynamic = 0x10,
            RequireFullSIC = 0x200,
            AutoImpersonate = 0x400,
            NoCustomMarshal = 0x2000,
            DisableAAA = 0x1000
        }

        [System.Runtime.InteropServices.DllImport("ole32.dll")]
        public static extern int CoInitializeSecurity(IntPtr pVoid, int
            cAuthSvc, IntPtr asAuthSvc, IntPtr pReserved1, RpcAuthnLevel level,
            RpcImpLevel impers, IntPtr pAuthList, EoAuthnCap dwCapabilities, IntPtr
            pReserved3);
    }
}
