#!/bin/bash

if [[ ! ( $1 && $2 ) ]] ; then
  echo "  USAGE: "
  echo "    bash SUBPRC_FY3CVASS_HDF5toBIN.bash FIL_IN FIL_OUT"
  exit 3
fi

fil_tp1=$1
fil_in1=${fil_tp1}
fil_tp2=$2
fil_out=${fil_tp2}

dir_out=`dirname fil_out`
[[ ! -d ${dir_out} ]] && mkdir -p ${dir_out}
[[ ! -d ${dir_out} ]] && { echo "DIR ${dir_out} does NOT exist" ; exit 3 ; }

echo ${fil_in1}
echo ${fil_out}

## To Define the SAT and Instriment.
DRSAT=${MM_SAT:-"FY3C"}
DRIST=${MM_IST:-"VASS"}
DRREEXT=${MM_REEXT:-"TRUE"}
 
## Load the functions defined.
DRDIRROT=${MM_DIRROOT}
source ${DRDIRROT}/scripts/MyFunctions.bash
source ${DRDIRROT}/scripts/Environment_${DRSAT}.bash
 
cmd_fix="SUBPRC_FY3BMWTS_HDF5toBIN".`date '+%Y%m%d.%H%M%S'`

cat > ${cmd_fix}.ncl << END_NCL
;------------------------------------------------------------------------
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/contributed.ncl"
;------------------------------------------------------------------------

  undef("Calymdhmx_from_dscnt")
  procedure Calymdhmx_from_dscnt ( dycnt[*]:snumeric, mscnt[*]:snumeric, iyer[*]:snumeric, imon[*]:snumeric,  \
                                    iday[*]:snumeric,  ihor[*]:snumeric, imin[*]:snumeric, isec[*]:snumeric )
  local utc_date, time, mscntx, dycntx, scdcnt
  begin
 
    mscntx  = tolong(mscnt) - 43200000
    dycntx  = where( mscntx .lt. 0, tointeger(dycnt), tointeger(dycnt) + 1 )
    mscntx  = where( mscntx .lt. 0, mscntx + 86400000, mscntx )
    
    time = dycntx
    time@units = "days since 2000-1-1 00:00:0.0"
    utc_date = cd_calendar(time, 0)
    iyer     = tointeger( utc_date(:, 0) )
    imon     = tointeger( utc_date(:, 1) )
    iday     = tointeger( utc_date(:, 2) )
    ihor     = tointeger( mscntx/3600000 )
    ihor     = where(ihor .ge. 24, 23, ihor)
    scdcnt   = tointeger( mscntx/1000 )
    imin     = mod(scdcnt, 3600)/60
    isec     = mod(scdcnt, 60) 
  end 

  loadscript("${MM_DIRTOOL}/procs/SUB_TOVSL1X_INDEX.ncl")

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
 
    fil_in1  = "${fil_in1}"
    fil_out  = "${fil_out}"

    if ( isfilepresent( fil_out ) ) then
      system("rm -rf "+fil_out)
    ;  print( "File "+fil_out+" existent, skip!!" )
    ;  exit
    end if   
 
    npnt     = 15
    nchn     = 4
    nadd     = 25
    nlen     = nchn + nadd
   
    print(fil_in1)
    f_var    = addfile(fil_in1, "r" )
    scln     = tointeger( f_var->Scnlin )
    nscn     = dimsizes(scln)   
    print(nscn+"  "+npnt+"  "+nchn+"  "+nadd)    

    aa       = f_var->scnlin_qc
    bb       = f_var->pixel_qc(:, 0)
    cc       = f_var->Earth_Obs_BT(2, :, 0)
    print(aa+"  "+bb+"  "+cc)
 
    ;print(f_var)
    ;exit

    ivar     = -1
    dout     = new((/nscn, npnt, nlen/), "integer", -9999)
    dout     = dout@_FillValue
    
;; 0-2: platform
    dout(:, :, 0:2)   = dout@_FillValue 
 
;; integer*4 sat_id 
    ivar   = IDSATX
    dout(:, :, ivar)    = 2
    
;; integer*4 instrument_id
    ivar   = IDISTX
    dout(:, :, ivar)    = 32
    
;; scanline number
    ivar   = IDSCLN
    do i = 0, nscn - 1
      dout(i, :, ivar)  = i + 1 
    end do
    
;; scan point number
    ivar   = IDSCPT
    do i = 0, npnt - 1
      dout(:, i, ivar)  = i + 1 
    end do

;;; times...
    tmp1   = f_var->Scnlin_daycnt
    tmp2   = f_var->Scnlin_mscnt
    ryer   = new(nscn, "integer", -9999)
    rmon   = ryer
    rday   = ryer
    rhor   = ryer
    rmin   = ryer
    rsec   = ryer
    Calymdhmx_from_dscnt( tmp1, tmp2, ryer, rmon, rday, rhor, rmin, rsec )

;; integer*4 obs_year
    ivar   = IDYEAR
    dout(:, :, ivar)  = conform( dout(:, :, ivar), ryer, 0 )
    
;; integer*4 obs_mon
    ivar   = IDMONX
    dout(:, :, ivar)  = conform( dout(:, :, ivar), rmon, 0 )
    
;; integer*4 obs_day
    ivar   = IDDAYX
    dout(:, :, ivar)  = conform( dout(:, :, ivar), rday, 0 )
         
;; integer*4 obs_hor
    ivar   = IDHOUR
    dout(:, :, ivar)  = conform( dout(:, :, ivar), rhor, 0 )
    
;; integer*4 obs_min 
    ivar   = IDMINU
    dout(:, :, ivar)  = conform( dout(:, :, ivar), rmin, 0 )
    
;; integer*4 obs_sec
    ivar   = IDSCND
    dout(:, :, ivar)  = conform( dout(:, :, ivar), rsec, 0 )
        
;; integer*4 obs_lat
    ivar   = IDLATX
    dddd   = f_var->Latitude 
    dout(:, :, ivar)  = tointeger( dddd*100 ) 
         
;; integer*4 obs_lon
    ivar   = IDLONX
    dddd   = f_var->Longitude 
    dout(:, :, ivar)  = tointeger( dddd*100 )
    delete(dddd) 
    
;; integer*4 surface_mark
    ivar   = IDTLND
    dout(:, :, ivar)  = tointeger( f_var->LandSeaMask )
 
;; integer*4 surface_height
    ivar   = IDZSFC
    dout(:, :, ivar)  = tointeger( f_var->Height )
    
;; integer*4 local_zenith
    ivar   = IDLZAX
    dout(:, :, ivar)  = tointeger( f_var->SensorZenith )
     
;; integer*4 local_azimuth
    ivar   = IDLAAX
    dout(:, :, ivar)  = tointeger( f_var->SensorAzimuth )
         
;; integer*4 solar_zenith
    ivar   = IDSZAX
    dout(:, :, ivar)  = tointeger( f_var->SolarZenith )
     
;; integer*4 solar_azimuth
    ivar   = IDSAAX
    dout(:, :, ivar)  = tointeger( f_var->SolarAzimuth )
         
;; integer*4 sat_scalti
    ivar   = IDZSAT
    dout(:, :, ivar)  = 83200
     
;; integer*4 obs_dataqual
    ivar   = IDFFLG
    dout(:, :, ivar)  = 35
     
;; integer*4 obs_bt(4)
    dddd   = f_var->Earth_Obs_BT
    do i = 0, nchn - 1
      ivar   = IDBTXX + i
      dout(:, :, ivar) = tointeger( dddd(i, :, :)*100 )
    end do
    delete(dddd)
     
;; integer*4 cld_frac 
    ivar   = IDCAMA + nchn - 1
    dout(:, :, ivar) = -9999
        
;; integer*4 prec_mask
    ivar   = IDRAIN + nchn - 1
    dout(:, :, ivar) = -9999    
        
    print((/dout(0, 0, :)/))  
    exit  
    fbindirwrite(fil_out, dout )
 
    delete(dout)
    
  end 
  
END_NCL

/home/f3cmon/ncl/bin/ncl ${cmd_fix}.ncl #> .todelete
rm -rf ${cmd_fix}.ncl
