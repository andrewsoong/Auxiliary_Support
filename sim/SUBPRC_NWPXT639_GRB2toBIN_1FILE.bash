#!/bin/bash 

### INPUT....
### $1: grib2-formatted T639 file with absolute dir
### $2: dir for binnary-formatted T639 file

if [[ ! ( $1 && $2 ) ]] ; then
  echo "USAGE:   "
  echo "bash SUBPRC_NWPXT639_GRB2toBIN_1FILE.bash T639-filname.grb2 T639-dir"
  exit 1
fi
 
cmd_fix="SUBPRC_T639_HDF5toBIN_1FILE".`date '+%Y%m%d.%H%M%S'`

cat > ${cmd_fix}.ncl << END_NCL
;------------------------------------------------------------------------
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/contributed.ncl"
;------------------------------------------------------------------------
  begin
    print("hello...")
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues
 
    fil_in     = "$1"
    dir_out    = "$2"
    fil_suf    = systemfunc("basename "+fil_in+" | rev | gawk -F. '{print \$1}' | rev " )
    fil_fix    = systemfunc("basename "+fil_in+" ."+fil_suf)
;    print(fil_fix)
    fil_out    = dir_out+"/"+fil_fix+".DAT"
;    print(fil_out)
;    exit        
 
    if ( isfilepresent( fil_out ) ) then
      continue
    end if

    f_var    = addfile(fil_in, "r")
;      print(f_var)
    xlon     = f_var->lon_0
;      print(xlon)
    xlat     = f_var->lat_0
;      print(xlat)
    xplv     = f_var->lv_ISBL0
;    	print(xplv/100)

; psfc    	
    dout  = f_var->PRES_P0_L1_GLL0(::-1, :)*0.01
    psfc  = dout
;      printMinMax(dout, True)
    fbindirwrite( fil_out, dout )

;; slp
    dout  = f_var->PRMSL_P0_L101_GLL0(::-1, :)*0.01
;      printMinMax(dout, True)
    fbindirwrite( fil_out, dout )

;; tsfc
    dout  = f_var->TMP_P0_L1_GLL0(::-1, :) 
;      printMinMax(dout, True)
    fbindirwrite( fil_out, dout )

;; satx
    dout  = f_var->TMP_P0_L103_GLL0(::-1, :) 
    t2m   = dout
;      printMinMax(dout, True)
    fbindirwrite( fil_out, dout )

;; uwnd
    dout  = f_var->UGRD_P0_L103_GLL0(::-1, :) 
;      printMinMax(dout, True)
    fbindirwrite( fil_out, dout )    	

;; vwnd
    dout  = f_var->VGRD_P0_L103_GLL0(::-1, :) 
;      printMinMax(dout, True)
    fbindirwrite( fil_out, dout ) 

;; saq
    RH    = f_var->RH_P0_L103_GLL0(::-1, :) 
    dout  = mixhum_ptrh(psfc, t2m, RH, -2)
;      printMinMax(dout, True)
    fbindirwrite( fil_out, dout ) 

;; ZSFC
    dout  = f_var->HGT_P0_L1_GLL0(::-1, :)*0.001
;      printMinMax(dout, True)
    fbindirwrite( fil_out, dout )
       
;; tlnd
    dout  = f_var->LAND_P0_L1_GLL0(::-1, :) 
;      printMinMax(dout, True)
    fbindirwrite( fil_out, dout )
      
;; tair
    do3d  = f_var->TMP_P0_L100_GLL0(:,::-1, :)            
;      printMinMax(do3d, True)
    fbindirwrite( fil_out, do3d )

;; h2ox
    do3d  = f_var->SPFH_P0_L100_GLL0(:,::-1, :)*1000. 
    do3d  = where(do3d .lt. 1.0e-4, 1.0e-4, do3d)           
;      printMinMax(do3d, True)
      fbindirwrite( fil_out, do3d )
 
  end 
 
END_NCL
 
/home/f3cmon/ncl/bin/ncl ${cmd_fix}.ncl > .todelete
rm -rf ${cmd_fix}.ncl 


