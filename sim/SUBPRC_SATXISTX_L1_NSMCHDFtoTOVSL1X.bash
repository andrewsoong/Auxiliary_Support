#!/bin/bash  -f

##==================================================================================================
##  OBJECT:
##      To to convert NSMCHDF formatted data (Actually MWTS/MWHS/MWRI/IRAS) to TOVSL1X format.
##
##  INPUTS:
##      1) DIR ROOT [default: command line]
##      2) SAT name [default: command line]
##      2) IST name [default: command line]
##      2) LEV name [default: command line]
##      2) FNC name [default: command line]
##      3) FIL_IN1  [default: command line]
##      4) FIL_L1D  [default: command line]
##
##  OUTPUTS:
##      1) formatted FIL_L1D  
##
##==================================================================================================

if [[ $# -eq 4 ]] ; then
  DROOT=$1
  DRTMP=$2
  DRSAT=`echo ${DRTMP} | gawk -F_  '{print $1}'`
  DRIST=`echo ${DRTMP} | gawk -F_  '{print $2}'`
  DRLEV=`echo ${DRTMP} | gawk -F_  '{print $3}'`
  DRFNC=`echo ${DRTMP} | gawk -F_  '{print $4}'`
  DRFIN=$3
  DFL1D=$4
elif [[ $# -eq 2 ]] ; then
  DROOT=${MM_DIRROOT}
  DRSAT=${MM_SAT}
  DRIST=${MM_IST}
  DRLEV=${MM_LEV}
  DRFNC=${MM_FNC}
  DRFIN=$1
  DFL1D=$2
else 
  echo "  USAGE: "
  echo "      bash SUBPRC_SATXISTX_L1_NSMCHDFtoTOVSL1X.bash ROT SAT_IST_LEV_FNC FIL_IN1 FIL_L1D"
  echo "    or  "
  echo "      bash SUBPRC_SATXISTX_L1_NSMCHDFtoTOVSL1X.bash FIL_IN1 FIL_L1D"
  echo "    if ENVs (ROT SAT IST LEV FNC) are setted..  "  
  exit 1
fi
echo "Esentials: ${DROOT} ${DRSAT} ${DRIST} ${DRLEV} ${DRFNC} ${DRFIN} ${DFL1D}"
Fil_OBSFix="${DRSAT}_${DRIST}_${DRLEV}_${DRFNC}"
echo "Fil_OBSFix: ${Fil_OBSFix}"
#exit
 
source ${DROOT}/scripts/comms/MyFunctions.bash
source ${DROOT}/scripts/comms/Environment_${DRSAT}.bash

eval ntmp=\$NCHN${DRIST}  ; let "NCHN=${ntmp:-54}"  ; printf "%s %4.4d\n" "NCHN: " ${NCHN}
eval ntmp=\$NPNT${DRIST}  ; let "NPNT=${ntmp:-15}"  ; printf "%s %4.4d\n" "NPNT: " ${NPNT}
eval ntmp=\$NADD${DRSAT}  ; let "NADD=${ntmp:-25}"  ; printf "%s %4.4d\n" "NADD: " ${NADD}
eval ntmp=\$NBIT${DRSAT}  ; let "NBIT=${ntmp:-4}"   ; printf "%s %4.4d\n" "NBIT: " ${NBIT}
let "NLEN=${NCHN} + ${NADD} " 
nsiz=`ls -l ${DRFIN} | gawk '{print $5}' `
let "NSCN=${nsiz} / ${NLEN} / ${NPNT} / ${NBIT}"
echo ${DRSAT} ${DRIST} ${NCHN} ${NPNT} ${NADD} ${NLEN} ${NSCN}
 
ymd_hm=`Get_ymd_hm_${DRSAT} ${DRFIN} ${DRSAT} ${DRIST} ${DRLEV} ${DRFNC}` 
cyer=`echo ${ymd_hm} | cut -c 1-4`
cmon=`echo ${ymd_hm} | cut -c 5-6`
chor=`echo ${ymd_hm} | cut -c 10-11`
echo ${ymd_hm} ${cyer} ${cmon} ${chor}
#exit
DRREEXT=${REEXT:-FALSE}

cmd_fix="${DROOT}/scripts/temps/SUBPRC_FY3CMWTS_NSMCHDF5toTOVSL1X"

cat > ${cmd_fix}.ncl << END_NCL
;------------------------------------------------------------------------
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "${DROOT}/scripts/comms/UDF.ncl"
;------------------------------------------------------------------------
 
  loadscript("${DROOT}/scripts/comms/SUB_TOVSL1X_INDEX.ncl")
  loadscript("${DROOT}/scripts/comms/SUB_ReadFunc_${DRSAT}.ncl")
  
  begin
    print("hello...")
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues 

;          character*12 plat_form 
;          integer*4 sat_id 
;          integer*4 instrument_id
;          integer*4 scan_line
;          integer*4 scan_fov
;          integer*4 obs_year
;          integer*4 obs_mon
;          integer*4 obs_day 
;          integer*4 obs_hor
;          integer*4 obs_min 
;          integer*4 obs_sec
;          integer*4 obs_lat 
;          integer*4 obs_lon
;          integer*4 surface_mark 
;          integer*4 surface_height
;          integer*4 local_zenith 
;          integer*4 local_azimuth 
;          integer*4 solar_zenith 
;          integer*4 solar_azimuth 
;          integer*4 sat_scalti 
;          integer*4 obs_dataqual 
;          integer*4 obs_bt(4) 
;          integer*4 cld_frac 
;          integer*4 prec_mask
 
    fil_in1  = "${DRFIN}"
    fil_out  = "${DFL1D}"
    
;;;; index for display.
    ix       = 0
    iy       = 0
    
    if ( isfilepresent( fil_out ) ) then
      if ( "${DRREEXT}" .eq. "TRUE" ) then
        system("rm -rf "+fil_out)
      else
        exit
      end if
    end if   
 
    npnt     = ${NPNT}
    nchn     = ${NCHN}
    nadd     = ${NADD}
    nlen     = ${NLEN}
    nscn     = ${NSCN}
;    print(npnt+" "+nchn+" "+nadd+" "+nlen+" "+nscn)
;    exit
 
    btmax      = ${BTMAX} / 100.
    btmin      = ${BTMIN} / 100.
    ibtend     = nchn - 1
    if ( "${DRSAT}${DRIST}" .eq. "FY3CVASS" .or. "${DRSAT}${DRIST}" .eq. "FY3CIRAS" )
      ibtend     = nchn - 7
    end if
   
    print(fil_in1)
    f_var    = addfile(fil_in1, "r" )
;    print(f_var)
;    exit

;;;; get the number of scan lines 
    readout  = ReadFunc_nscn( f_var )
    if ( readout ) then
      nscn   = readout@nscn
    else
      print("Failed to read scan number")
      exit
    end if
    print("NSCN: "+nscn)
;    exit
 
;;;; allocate space for output data
    dout     = new((/nscn, npnt, nlen/), "integer", -9999)
    dout     = dout@_FillValue
    qscn     = new((/nscn, npnt, 4/), "integer", -9999)
    qchn     = new((/nscn, npnt/), "integer", -9999)
    
;; 0-2: platform
    dout(:, :, 0:2)   = dout@_FillValue 

;; integer*4 sat_id 
    csats=(/"FY3A", "FY3B", "FY3C"/)
    isats=(/  1,      2,      3   /)
    indxx=ind(csats .eq. "${DRSAT}")
    dout(:, :, IDSATX)    = isats( indxx )
    delete(indxx)
    
;; integer*4 instrument_id
    cists=(/"MWTS", "MWHS", "MWRI", "IRAS"/)
    iists=(/  32  ,   31,     33,     30/)
    indyy=ind(cists .eq. "${DRIST}")
    dout(:, :, IDISTX)    = iists(indyy)
    delete(indyy)

;; scanline number
    do i = 0, nscn - 1
      dout(i, :, IDSCLN)  = i + 1 
    end do
;    print("IDSCLN: "+dout(ix, iy, IDSCLN))
    
;; scan point number
    do i = 0, npnt - 1
      dout(:, i, IDSCPT)  = i + 1 
    end do
;    print("IDSCPT: "+dout(ix, iy, IDSCPT))

;;;; read time information
    readout  = ReadFunc_time( f_var )
    if ( readout ) then
      ryer     = readout@ryer
      rmon     = readout@rmon
      rday     = readout@rday
      rhor     = readout@rhor
      rmin     = readout@rmin
      rsec     = readout@rsec
      delete(readout)
 
      difhor   = abs( rhor(0) - ${chor} )       
      if ( difhor .gt. 1 ) then
        ADJUST_ymdhm ( difhor, ryer, rmon, rday, rhor, rmin, rsec )
      end if
    else
      print("Failed to read Time")
      print(fil_in)
      exit
    end if

    dout(:, :, IDYEAR)  = tointeger( conform( dout(:, :, 0), ryer, 0 ) )
    print("IDYEAR: "+dout(ix, iy, IDYEAR))
    dout(:, :, IDMONX)  = tointeger( conform( dout(:, :, 0), rmon, 0 ) )
    print("IDMONX: "+dout(ix, iy, IDMONX)) 
    dout(:, :, IDDAYX)  = tointeger( conform( dout(:, :, 0), rday, 0 ) )
    print("IDDAYX: "+dout(ix, iy, IDDAYX))
    dout(:, :, IDHOUR)  = tointeger( conform( dout(:, :, 0), rhor, 0 ) )
    print("IDHOUR: "+dout(ix, iy, IDHOUR))
    dout(:, :, IDMINU)  = tointeger( conform( dout(:, :, 0), rmin, 0 ) )
    print("IDMINU: "+dout(ix, iy, IDMINU))
    dout(:, :, IDSCND)  = tointeger( conform( dout(:, :, 0), rsec, 0 ) )
    print("IDSCND: "+dout(ix, iy, IDSCND))
;    exit

;;;; latitude
    readout  = ReadFunc_lat( f_var )
    if ( readout ) then
      lat2d = readout@lat
      delete(readout)
    else
      print("Failed to read lat")
      exit
    end if
    dout(:, :, IDLATX) = tointeger( lat2d*100 )
    print("IDLATX: "+dout(ix, iy, IDLATX))
;    exit 

;;;; Longitude
    readout  = ReadFunc_lon( f_var )
    if ( readout ) then
      lon2d  = readout@lon
      delete(readout)
    else
      print("Failed to read lon")
      exit 
    end if
    dout(:, :, IDLONX) = tointeger( lon2d*100 )
    print("IDLONX: "+dout(ix, iy, IDLONX))
;    exit

;; land-sea mask
    readout  = ReadFunc_lsmsk( f_var )
    if ( readout ) then
      lsm2d  = readout@lsmsk
      delete(readout)
    else
      print("Failed to read land sea mask.")
      exit 
    end if
    dout(:, :, IDTLND) = tointeger( lsm2d )
    print("IDTLND: "+dout(ix, iy, IDTLND))
;    exit
     
;; integer*4 surface_height
    readout  = ReadFunc_zsfc( f_var )
    if ( readout ) then
      zsfc2d  = readout@zsfc
      delete(readout)
    else
      print("Failed to read land sea mask.")
      exit 
    end if
    dout(:, :, IDZSFC) = tointeger( zsfc2d )  
    print("IDZSFC: "+dout(ix, iy, IDZSFC))
;    exit

;;;; Local Zenith Angle
    readout  = ReadFunc_lza( f_var )
    if ( readout ) then
      lza2d = readout@lza
      delete(readout)
    else
      print("Failed to read lza")
      exit 
    end if
    dout(:, :, IDLZAX) = tointeger( lza2d*100 ) 
    print("IDLZAX: "+dout(ix, iy, IDLZAX))
;    exit
        
;; integer*4 local_azimuth
    readout  = ReadFunc_laa( f_var )
    if ( readout ) then
      laa2d = readout@laa
      delete(readout)
    else
      print("Failed to read laa")
      exit 
    end if
    dout(:, :, IDLAAX) = tointeger( laa2d*100 ) 
    print("IDLAAX: "+dout(ix, iy, IDLAAX))
;    exit

;;;; Solar Zenith Angle
    readout  = ReadFunc_sza( f_var )
    if ( readout ) then
      sza2d  = readout@sza
      delete(readout)
    else
      print("Failed to read lza")
      exit 
    end if
    dout(:, :, IDSZAX) = tointeger( sza2d*100 ) 
    print("IDSZAX: "+dout(ix, iy, IDSZAX))
;    exit

;;;; Solar Azimuth Angle
    readout  = ReadFunc_saa( f_var )
    if ( readout ) then
      saa2d  = readout@saa
      delete(readout)
    else
      print("Failed to read laa")
      exit 
    end if
    dout(:, :, IDSAAX) = tointeger( saa2d*100 )  
    print("IDSAAX: "+dout(ix, iy, IDSAAX))
;    exit
     
;; integer*4 sat_scalti
    dout(:, :, IDZSAT)  = 83200
    print("IDZSAT: "+dout(ix, iy, IDZSAT))
 
;;;; scan line for calibration, only for IRAS
    calline  = new(nscn, "integer", -9999)
    calline  = 0
    readout  = ReadFunc_caline( f_var )
    if ( readout ) then
      caline = readout@caline
      if ( all( .not. ismissing(caline) ) ) then
        calline(caline+0)   = 1
        calline(caline+1)   = 1   
      end if
      delete(caline)
    end if 
;    print(calline)
;    exit 
             
;; integer*4 obs_dataqual
;;;; fov Flags
    readout  = ReadFunc_qcsc( f_var )
    if ( readout ) then
      qcscan = readout@qcsc
    else
      print("Failed to read qcscan")
      exit
    end if
    print("MaxMin QCSCAN: "+max(qcscan)+"  "+min(qcscan))
;    print(qcscan)
;    exit
    
;;;; get nsmctmp fov flags
    qcscns    = new((/nscn, 4/), "integer", -9999)
    qcscns    = 0
    indflg    = ind( qcscan .gt. 0 )
    if ( all( .not. ismissing(indflg) ) ) then
      qcscns(indflg, :) = QA_FOV_NSMCHDF2TOVSTMP( qcscan(indflg), "${DRSAT}${DRIST}" )
     ;print("TEST FOV "+qcscan(indflg)+" "+qcscns(indflg, 0)+" "+qcscns(indflg, 1)+" "+qcscns(indflg, 2)+" "+qcscns(indflg, 3))
    end if
    delete(indflg)
    indflg    = ind( calline .gt. 0 )
    if ( all( .not. ismissing(indflg) ) ) then
      qcscns(indflg, 0:1)  = 1
    end if
    delete(indflg)
;    print("TEST FOV "+qcscan(::10)+" "+qcscns(::10, 0)+" "+qcscns(::10, 1)+" "+qcscns(::10, 2)+" "+qcscns(::10, 3))
;    print("TEST FOV "+qcscan+" "+qcscns(:, 0)+" "+qcscns(:, 1)+" "+qcscns(:, 2)+" "+qcscns(:, 3))    
;    exit

;;;; channel flags,, convert to chn later..
    readout  = ReadFunc_qcch( f_var )
    if ( readout ) then
      qcchan  = readout@qcch
    else
      print("Failed to read qcchan")
      exit
    end if
    qcchan  = where( ismissing( qcchan ) .or. qcchan .gt. 0, 1, qcchan)
    indflg    = ind( calline .gt. 0 )
    if ( all( .not. ismissing(indflg) ) ) then
      qcchan(indflg)  = 1
    end if
    delete(indflg)
    print("MaxMin QCCHAN: "+max(qcchan)+"  "+min(qcchan))
;    print(qcchan(::10))
;    exit
 
    do ipnt = 0, npnt - 1
      qscn(:, ipnt, :)  = qcscns
      qchn(:, ipnt)     = qcchan
    end do
;    exit
     
;;;; bts
    readout  = ReadFunc_bts( f_var ) 
    if ( readout ) then
      bts3d = readout@bts
      delete(readout)
    else
      print("Failed to read bts")
      exit
    end if
    dout(:, :, IDBTXX:IDBTXX + nchn - 1) = tointeger( bts3d*100 )
    print("IDBTXX: "+dout(ix, iy, IDBTXX:IDBTXX + nchn - 1))
;    exit

;; integer*4 cld_frac 
    isread    = False
    dout(:, :, IDCAMA + nchn - 1) = -9999
    print("IDCAMA: "+dout(ix, iy, IDCAMA + nchn - 1))
            
;; integer*4 prec_mask
    isread    = False
    dout(:, :, IDRAIN + nchn - 1) = -9999    
    print("IDRAIN: "+dout(ix, iy, IDRAIN + nchn - 1))

    ;print("BF: "+num(qscn .gt. 0)+"  "+num(qchn .gt. 0))
    
;;;; modify the qc by vars
;    latflv  = max( (/  90.1, max( abs( lat2d ) ) - 1.0 /) )
;    lonflv  = max( (/ 360.1, max( abs( lon2d ) ) - 1.0 /) )
;    lzaflv  = max( (/ 180.1, max( abs( lza2d ) ) - 1.0 /) )
;    laaflv  = max( (/ 180.1, max( abs( laa2d ) ) - 1.0 /) )
;    szaflv  = max( (/ 180.1, max( abs( sza2d ) ) - 1.0 /) )
;    saaflv  = max( (/ 180.1, max( abs( saa2d ) ) - 1.0 /) )
;    btsflv  = max( (/ 400.1, max( abs( bts3d ) ) - 1.0 /) )
;    print(latflv+" "+lonflv+" "+lzaflv+" "+laaflv+" "+szaflv+" "+saaflv+" "+btsflv)

;;;; for geo-info
    qscn(:, :, 3)    = where( ismissing(lat2d) .or. abs( lat2d ) .gt.  90., 1, qscn(:, :, 3) ) 
    ;print("1F: "+num(qscn .gt. 0)+"  "+num(qchn .gt. 0))
    qscn(:, :, 3)    = where( ismissing(lon2d) .or. abs( lon2d ) .gt. 360., 1, qscn(:, :, 3) ) 
    ;print("2F: "+num(qscn .gt. 0)+"  "+num(qchn .gt. 0))
    qscn(:, :, 3)    = where( ismissing(lza2d) .or. abs( lza2d ) .gt. 180., 1, qscn(:, :, 3) )
    ;print("3F: "+num(qscn .gt. 0)+"  "+num(qchn .gt. 0))     
    qscn(:, :, 3)    = where( ismissing(laa2d) .or. abs( laa2d ) .gt. 180., 1, qscn(:, :, 3) ) 
    ;print("4F: "+num(qscn .gt. 0)+"  "+num(qchn .gt. 0))
    qscn(:, :, 3)    = where( ismissing(sza2d) .or. abs( sza2d ) .gt. 180., 1, qscn(:, :, 3) ) 
    ;print("5F: "+num(qscn .gt. 0)+"  "+num(qchn .gt. 0))
    qscn(:, :, 3)    = where( ismissing(saa2d) .or. abs( saa2d ) .gt. 180., 1, qscn(:, :, 3) ) 
    ;print("6F: "+num(qscn .gt. 0)+"  "+num(qchn .gt. 0))
    
;;;; for bts
    do ichn = 0, ibtend 
      bts2d = bts3d(:, :, ichn)
      qscn(:, :, 1)  = where( ismissing(bts2d) .or. bts2d .lt. btmin .or. bts2d .gt. btmax, 1, qscn(:, :, 1) )
      qchn           = where( ismissing(bts2d) .or. bts2d .lt. btmin .or. bts2d .gt. btmax, 1, qchn )
    end do  
    qscn(:, :, 0)    = where( qscn(:, :, 1) .gt. 0 .or. qscn(:, :, 3) .gt. 0, 1, qscn(:, :, 0) )

    zzzz    = tointeger(2^nchn ) - 1
    qchn    = where( qchn .gt. 0, zzzz, qchn )
    qchn    = where( qscn(:, :, 1) .gt. 0, zzzz, qchn )
    qchn    = qchn * 16
    
    ;print("AF: "+num(qscn .gt. 0)+"  "+num(qchn .gt. 0))
    dout(:, :, IDFFLG) = CNVTINT_bin2dec( qscn ) + qchn
    print("IDFFLG: "+dout(ix, iy, IDFFLG))
    ;print(qscn(::10, 0, 0)+"  "+qscn(::10, 0, 1)+"  "+qscn(::10, 0, 2)+"  "+qscn(::10, 0, 3)+"  "+dout(::10, 0, IDFFLG))
;    print(qcscan+" "+qcchan+" "+qscn(:, 0, 0)+"  "+qscn(:, 0, 1)+"  "+qscn(:, 0, 2)+"  "+qscn(:, 0, 3)+"  "+dout(:, 0, IDFFLG))
;    print((/dout(ix, iy, :)/))  
;    exit  
    if ( isfilepresent( fil_out ) ) then
      system("rm -rf "+fil_out)
    end if
    fbindirwrite(fil_out, dout )
 
    delete(dout)
    
  end 
  
END_NCL

ncl ${cmd_fix}.ncl > .todelete
rm -rf ${cmd_fix}.ncl
