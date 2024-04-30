import re
import json
import traceback

from pymavlink import mavutil

from dbtest import *
from database import create_connection, close_connection


def read_all_and_update_db(log_folder_path):
    print("read_all_and_update_db started")

    # Connect to database table
    conn = create_connection()
    ret_val = create_tables(conn)

    if not ret_val:
        print("Error in creating table")
        return
    
    count = 0

    mlog = mavutil.mavlink_connection(log_folder_path)

    while True:
        try:
            # Get the type of the message
            msg = mlog.recv_msg()
            if msg is None:
                break
                
            match msg.get_type():
                case None:
                    break
                case 'FMT':
                    ret_val = insert_FMT_data_record(conn, msg.Type, msg.Length, msg.Name, msg.Format, msg.Columns)
                    print(msg)
                    break
                case 'PARM':
                    ret_val = insert_PARM_data_record(conn, msg.TimeUS, msg.Name, msg.Value, msg.Default)
                case 'MSG':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Message)

                case 'IMU':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.GyrX, msg.GyrY, msg.GyrZ, msg.AccX, msg.AccY, msg.AccZ, msg.EG, msg.EA, msg.T, msg.GH, msg.AH, msg.GHz, msg.AHz)
                case 'RATE':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.RDes, msg.R , msg.ROut, msg.PDes, msg.P , msg.POut, msg.YDes, msg.Y , msg.YOut, msg.ADes, msg.A , msg.AOut)
                case'ATSC':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.AngPScX, msg.AngPScY, msg.AngPScZ)
                case 'FMTU':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.FmtType, msg.UnitIds, msg.MultIds)
                case 'CTRL':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.RMSRollP, msg.RMSRollD, msg.RMSPitchP, msg.RMSPitchD, msg.RMSYaw)
                case 'MOTB':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.LiftMax, msg.BatVolt, msg.ThLimit, msg.ThrAvMx, msg.FailFlags)
                case 'QTUN':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.ThI, msg.ABst, msg.ThO, msg.ThH, msg.DAlt, msg.Alt, msg.BAlt, msg.DCRt, msg.CRt, msg.TMix, msg.Sscl, msg.Trn, msg.Ast)
                case 'PSCN':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.TPN, msg.PN, msg.DVN, msg.TVN, msg.VN, msg.DAN, msg.TAN, msg.AN)
                case 'PSCE':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.TPE, msg.PE, msg.DVE, msg.TVE, msg.VE, msg.DAE, msg.TAE, msg.AE)
                case 'ESC':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Instance, msg.RPM, msg.RawRPM, msg.Volt, msg.Curr, msg.Temp, msg.CTot, msg.MotTemp, msg.Err)
                case 'GPS':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.I, msg.Status, msg.GMS, msg.GWk, msg.NSats, msg.HDop, msg.Lat, msg.Lng, msg.Alt, msg.Spd, msg.GCrs, msg.VZ, msg.Yaw, msg.U)
                case 'GPA':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.I, msg.VDop, msg.HAcc, msg.VAcc, msg.SAcc, msg.YAcc, msg.VV, msg.SMS, msg.Delta, msg.Und)
                case 'RFND':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Instance, msg.Dist, msg.Stat, msg.Orient)
                case 'ATT':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.DesRoll, msg.Roll, msg.DesPitch, msg.Pitch, msg.DesYaw, msg.Yaw, msg.ErrRP, msg.ErrYaw, msg.AEKF)
                case 'PIQR':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'PIQP':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'PIQY':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'PIQA':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'PIDN':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'PIDE':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'PIDR':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'PIDP':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'PIDY':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'PIDS':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tar, msg.Act, msg.Err, msg.P, msg.I, msg.D, msg.FF, msg.Dmod, msg.SRate, msg.Limit)
                case 'XKF1':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.Roll, msg.Pitch, msg.Yaw, msg.VN, msg.VE, msg.VD, msg.dPD, msg.PN, msg.PE, msg.PD, msg.GX, msg.GY, msg.GZ, msg.OH)
                case 'XKF2':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.AX, msg.AY, msg.AZ, msg.VWN, msg.VWE, msg.MN, msg.ME, msg.MD, msg.MX, msg.MY, msg.MZ, msg.IDX, msg.IDY, msg.IS)
                case 'XKF3':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.IVN, msg.IVE, msg.IVD, msg.IPN, msg.IPE, msg.IPD, msg.IMX, msg.IMY, msg.IMZ, msg.IYAW, msg.IVT, msg.RErr, msg.ErSc)
                case 'XKF4':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.SV, msg.SP, msg.SH, msg.SM, msg.SVT, msg.errRP, msg.OFN, msg.OFE, msg.FS, msg.TS, msg.SS, msg.GPS, msg.PI)
                case 'XKF5':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.NI, msg.FIX, msg.FIY, msg.AFI, msg.HAGL, msg.offset, msg.RI, msg.rng, msg.Herr, msg.eAng, msg.eVel, msg.ePos)
                case 'XKFS':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.MI, msg.BI, msg.GI, msg.AI, msg.SS)
                case 'XKQ':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.Q1, msg.Q2, msg.Q3, msg.Q4)
                case 'XKV1':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.V00, msg.V01, msg.V02, msg.V03, msg.V04, msg.V05, msg.V06, msg.V07, msg.V08, msg.V09, msg.V10, msg.V11)
                case 'XKV2':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.V12, msg.V13, msg.V14, msg.V15, msg.V16, msg.V17, msg.V18, msg.V19, msg.V20, msg.V21, msg.V22, msg.V23)
                case 'AHR2':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Roll, msg.Pitch, msg.Yaw, msg.Alt, msg.Lat, msg.Lng, msg.Q1, msg.Q2, msg.Q3, msg.Q4)
                case 'POS':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Lat, msg.Lng, msg.Alt, msg.RelHomeAlt, msg.RelOriginAlt)
                case 'CTUN':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.NavRoll, msg.Roll, msg.NavPitch, msg.Pitch, msg.ThO, msg.RdrOut, msg.ThD, msg.As, msg.SAs, msg.E2T, msg.GU)
                case 'FTN':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.I, msg.NDn, msg.NF1, msg.NF2, msg.NF3, msg.NF4, msg.NF5, msg.NF6, msg.NF7, msg.NF8, msg.NF9, msg.NF10, msg.NF11, msg.NF12)
                case 'NTUN':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Dist, msg.TBrg, msg.NavBrg, msg.AltE, msg.XT, msg.XTi, msg.AsE, msg.TLat, msg.TLng, msg.TAW, msg.TAT, msg.TAsp)
                case 'RCIN':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C1, msg.C2, msg.C3, msg.C4, msg.C5, msg.C6, msg.C7, msg.C8, msg.C9, msg.C10, msg.C11, msg.C12, msg.C13, msg.C14)
                case 'RCI2':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C15, msg.C16, msg.OMask)
                case 'RCOU':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C1, msg.C2, msg.C3, msg.C4, msg.C5, msg.C6, msg.C7, msg.C8, msg.C9, msg.C10, msg.C11, msg.C12, msg.C13, msg.C14)
                case 'RCO2':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C15, msg.C16, msg.C17, msg.C18)
                case 'AETR':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Ail, msg.Elev, msg.Thr, msg.Rudd, msg.Flap, msg.SS)
                case 'VIBE':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.IMU, msg.VibeX, msg.VibeY, msg.VibeZ, msg.Clip)
                case 'ISBD':
                    # TODO : Need better way to handle arrays
                    #ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.N, msg.seqno, msg.x, msg.y, msg.z)
                    a = 0
                case 'EFI':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.LP, msg.Rpm, msg.SDT, msg.ATM, msg.IMP, msg.IMT, msg.ECT, msg.OilP, msg.OilT, msg.FP, msg.FCR, msg.CFV, msg.TPS, msg.IDX)
                case 'PSCD':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.TPD, msg.PD, msg.DVD, msg.TVD, msg.VD, msg.DAD, msg.TAD, msg.AD)
                case 'EFIS':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.ES1, msg.ES2, msg.CS1, msg.CS2, msg.ETS, msg.ATS, msg.APS, msg.TSS, msg.CRF, msg.AKF, msg.Up, msg.ThO, msg.ThM)
                case 'EFI2':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Healthy, msg.ES, msg.GE, msg.CSE, msg.TS, msg.FPS, msg.OPS, msg.DS, msg.MS, msg.DebS, msg.SPU, msg.IDX)
                case 'ECYL':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Inst, msg.IgnT, msg.InjT, msg.CHT, msg.EGT, msg.Lambda, msg.CHT2, msg.EGT2, msg.IDX)
                case 'MAG':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.I, msg.MagX, msg.MagY, msg.MagZ, msg.OfsX, msg.OfsY, msg.OfsZ, msg.MOX, msg.MOY, msg.MOZ, msg.Health, msg.S)
                case 'ARSP':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.I, msg.Airspeed, msg.DiffPress, msg.Temp, msg.RawPress, msg.Offset, msg.U, msg.H, msg.Hp, msg.TR, msg.Pri)
                case 'BARO':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.I, msg.Alt, msg.Press, msg.Temp, msg.CRt, msg.SMS, msg.Offset, msg.GndTemp, msg.Health)
                case 'POWR':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Vcc, msg.VServo, msg.Flags, msg.AccFlags, msg.Safety, msg.MTemp, msg.MVolt, msg.MVmin, msg.MVmax)
                case 'BAT':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Instance, msg.Volt, msg.VoltR, msg.Curr, msg.CurrTot, msg.EnrgTot, msg.Temp, msg.Res, msg.RemPct)
                case 'HEAT':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Temp, msg.Targ, msg.P, msg.I, msg.Out)
                case 'STAT':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.isFlying, msg.isFlyProb, msg.Armed, msg.Safety, msg.Crash, msg.Still, msg.Stage, msg.Hit)
                case 'RPM':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.rpm1, msg.rpm2)
                case 'AOA':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.AOA, msg.SSA)
                case 'UNIT':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Id, msg.Label)
                case 'DSF':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Dp, msg.Blk, msg.Bytes, msg.FMn, msg.FMx, msg.FAv)
                case 'MAV':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.chan, msg.txp, msg.rxp, msg.rxdp, msg.flags, msg.ss, msg.tf)
                case 'XKT':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.Cnt, msg.IMUMin, msg.IMUMax, msg.EKFMin, msg.EKFMax, msg.AngMin, msg.AngMax, msg.VMin, msg.VMax)
                case 'TERR':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Status, msg.Lat, msg.Lng, msg.Spacing, msg.TerrH, msg.CHeight, msg.Pending, msg.Loaded, msg.ROfs)
                case 'ISBH':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.N, msg.type, msg.instance, msg.mul, msg.smp_cnt, msg.SampleUS, msg.smp_rate)
                case 'RAD':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.RSSI, msg.RemRSSI, msg.TxBuf, msg.Noise, msg.RemNoise, msg.RxErrors, msg.Fixed)
                case 'MULT':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Id, msg.Mult)
                case 'VER':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.BT, msg.BST, msg.Maj, msg.Min, msg.Pat, msg.FWT, msg.GH, msg.FWS, msg.APJ)
                case 'CMD':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.CTot, msg.CNum, msg.CId, msg.Prm1, msg.Prm2, msg.Prm3, msg.Prm4, msg.Lat, msg.Lng, msg.Alt, msg.Frame)
                case 'FNCE':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Tot, msg.Seq, msg.Type, msg.Lat, msg.Lng, msg.Count, msg.Radius)
                case 'MODE':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Mode, msg.ModeNum, msg.Rsn)
                case 'ORGN':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Type, msg.Lat, msg.Lng, msg.Alt)
                case 'RFRF':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.FTypes, msg.Slow)
                case 'RFRH':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.TF)
                case 'RFRN':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.HLat, msg.HLon, msg.HAlt, msg.E2T, msg.AM, msg.TX, msg.TY, msg.TZ, msg.VC, msg.EKT, msg.Flags)
                case 'RISH':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.LR, msg.PG, msg.PA, msg.LD, msg.AC, msg.GC)
                case 'RISI':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.DVX, msg.DVY, msg.DVZ, msg.DAX, msg.DAY, msg.DAZ, msg.DVDT, msg.DADT, msg.Flags, msg.I)
                case 'RBRI':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.LastUpdate, msg.Alt, msg.H, msg.I)
                case 'RGPH':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.NumInst, msg.Primary)
                case 'RGPI':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.OX, msg.OY, msg.OZ, msg.Lg, msg.Flags, msg.Stat, msg.NSats, msg.I)
                case 'RGPJ':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.TS, msg.VX, msg.VY, msg.VZ, msg.SA, msg.Y, msg.YA, msg.YT, msg.Lat, msg.Lon, msg.Alt, msg.HA, msg.VA, msg.HD, msg.I)
                case 'RBRH':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.Primary, msg.NumInst)
                case 'RMGH':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.Dec, msg.Avail, msg.NumInst, msg.AutoDec, msg.NumEna, msg.LOE, msg.C, msg.FUsable)
                case 'RMGI':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.LU, msg.OX, msg.OY, msg.OZ, msg.FX, msg.FY, msg.FZ, msg.UFY, msg.H, msg.HSF, msg.I)
                case 'RASH':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.Primary, msg.NumInst)
                case 'RRNH':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.GCl, msg.MaxD, msg.NumSensors)
                case 'RRNI':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.PX, msg.PY, msg.PZ, msg.Dist, msg.Orient, msg.Status, msg.I)
                case 'RMGI':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.LU, msg.OX, msg.OY, msg.OZ, msg.FX, msg.FY, msg.FZ, msg.UFY, msg.H, msg.HSF, msg.I)
                case 'RASI':
                    # TODO : getting error while reading 'PD'
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.UpdateMS, msg.H, msg.Use, msg.I)
                case 'STAK':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.TimeUS, msg.Id, msg.Pri, msg.Total, msg.Free, msg.Name)
                case 'FILE':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.FileName, msg.Offset, msg.Length, msg.Data)
                case 'RMGI':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.LU, msg.OX, msg.OY, msg.OZ, msg.FX, msg.FY, msg.FZ, msg.UFY, msg.H, msg.HSF, msg.I)
                case 'IOMC':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.RSErr, msg.Mem, msg.TS, msg.NPkt, msg.Nerr, msg.Nerr2, msg.NDel)
                case 'CAND':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.NodeId, msg.UID1, msg.UID2, msg.Name, msg.Major, msg.Minor, msg.Version)
                case 'PM':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.LR, msg.NLon, msg.NL, msg.MaxT, msg.Mem, msg.Load, msg.ErrL, msg.IntE, msg.ErrC, msg.SPIC, msg.I2CC, msg.I2CI, msg.Ex)
                case 'XKY0':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.YC, msg.YCS, msg.Y0, msg.Y1, msg.Y2, msg.Y3, msg.Y4, msg.W0, msg.W1, msg.W2, msg.W3, msg.W4)
                case 'XKY1':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.IVN0, msg.IVN1, msg.IVN2, msg.IVN3, msg.IVN4, msg.IVE0, msg.IVE1, msg.IVE2, msg.IVE3, msg.IVE4,)
                case 'ARM':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.ArmState, msg.ArmChecks, msg.Forced, msg.Method)
                case 'EV':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.Id)
                case 'MAVC':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.TS, msg.TC, msg.SS, msg.SC, msg.Fr, msg.Cmd, msg.P1, msg.P2, msg.P3, msg.P4, msg.X, msg.Y, msg.Z, msg.Res, msg.WL)
                case 'XKFM':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.C, msg.OGNM, msg.GLR, msg.ALR, msg.GDR, msg.ADR)
                case 'REV3':
                    ret_val = insert_DATA_record(conn, msg.get_type(), "0", msg.Event)
                case 'LAND':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.stage, msg.f1, msg.f2, msg.slope, msg.slopeInit, msg.altO, msg.fh)
                case 'TECS':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.h, msg.dh, msg.hdem, msg.dhdem, msg.spdem, msg.sp, msg.dsp, msg.ith, msg.iph, msg.th, msg.ph, msg.dspdem, msg.w, msg.f)
                case 'TEC2':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.pmax, msg.pmin, msg.KErr, msg.PErr, msg.EDelta, msg.LF, msg.hdem1, msg.hdem2)
                case 'CMDI':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.CId, msg.TSys, msg.TCmp, msg.cur, msg.cont, msg.Prm1, msg.Prm2, msg.Prm3, msg.Prm4, msg.Lat, msg.Lng, msg.Alt, msg.F)
                case 'QPOS':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.State, msg.Dist, msg.TSpd, msg.TAcc, msg.OShoot)
                case 'RELY':
                    ret_val = insert_DATA_record(conn, msg.get_type(), msg.TimeUS, msg.State)
                case _:
                    # Debug only
                    print(msg)
                    if count > 15:
                        break
                    count += 1

            if not ret_val:
                print("Error in inserting data : " + msg.get_type())
                break
        except Exception as e:
            logger.info(msg)
            logger.debug(e)
            traceback.print_exc()
            break
    
    close_connection(conn)
    
    print("read_all_and_update_db ended")

